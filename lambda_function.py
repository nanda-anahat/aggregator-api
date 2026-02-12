import boto3
import json
import logging
import asyncio
import aiohttp

from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def validate_id(raw_id):
    """
    Validate user ids
    """
    if raw_id is None:
        raise ValueError("User_Id is required")

    try:
        user_id = int(raw_id)
    except (ValueError, TypeError):
        raise ValueError("User Id must be a valid integer")

    if user_id <= 0:
        raise ValueError("User Id must be a valid integer")

    return user_id


def fetch_ssm_parameters(parameter_names):
    """
    Fetch multiple SSM parameters and collect any failures
    """
    ssm_client = boto3.client("ssm")
    parameters = {}

    for param_name in parameter_names:
        try:
            response = ssm_client.get_parameter(Name=param_name, WithDecryption=True)
            parameters[param_name] = response["Parameter"]["Value"]
        except (ClientError, ParamValidationError) as e:
            logger.error("Error retrieving SSM parameter %s: %s", param_name, str(e))
            raise ValueError(
                f"Error retrieving SSM parameter {param_name}: {str(e)}"
            ) from e
    return parameters


async def fetch_url(session, url):
    """
    Asynchronous invocation to fetch url
    """
    async with session.get(url) as response:
        if response.status != 200:
            logger.error("HTTP %d from %s", response.status, url)
            raise aiohttp.ClientResponseError(
                request_info=response.request_info,
                history=response.history,
                status=response.status,
                message="Non-200 response from external API",
            )
        data = await response.json()
        logger.info("Successfully fetched %d records from:%s", len(data), url)
        return data


async def fetch_posts_comments(user_id, task_external_api):
    """
    Asynchronous invocation to fetch posts and comments
    """
    posts_url = "%s/posts?userId=%d" % (task_external_api, user_id)
    comments_url = "%s/comments" % task_external_api

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        logger.info("Parallel requests for user_id:%d", user_id)
        posts, comments = await asyncio.gather(
            fetch_url(session, posts_url),
            fetch_url(session, comments_url),
            return_exceptions=False,
        )
        logger.info(
            "Processing complete. Posts:%d, Comments:%d", len(posts), len(comments)
        )
        return posts, comments


def build_hierarchical_data(user_id, posts, comments):
    """
    Constructing hierarchical data from posts and comments
    """
    logger.info("Building hierarchical data for user_id:%d", user_id)
    comments_by_post = {}
    for comment in comments:
        post_id = comment.get("postId")
        if post_id not in comments_by_post:
            comments_by_post[post_id] = []
        comments_by_post[post_id].append(
            {
                "id": comment.get("id"),
                "name": comment.get("name"),
                "email": comment.get("email"),
                "body": comment.get("body"),
            }
        )
    hierarchical_posts = []
    for post in posts:
        post_id = post.get("id")
        hierarchical_post = {
            "id": post_id,
            "title": post.get("title"),
            "body": post.get("body"),
            "comments": comments_by_post.get(post_id, []),
        }
        hierarchical_posts.append(hierarchical_post)

    response = {
        "userId": user_id,
        "posts": hierarchical_posts,
        "metadata": {
            "totalPosts": len(hierarchical_posts),
            "totalComments": sum(len(post["comments"]) for post in hierarchical_posts),
        },
    }
    logger.info(
        "Hierarchical Response built. Posts:%d, Comments : %d",
        response["metadata"]["totalPosts"],
        response["metadata"]["totalComments"],
    )

    return response


def success_response(body):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }


def error_response(status_code, message):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({"error": message}),
    }


def lambda_handler(event, context):
    """
    Entry Method for Aggregator API invocation to return hierarchically aggregated data
    """
    try:
        logger.info("Aggregator API: %s", json.dumps(event))
        path_parameters = event.get("pathParameters") or {}
        raw_id = path_parameters.get("id")
        try:
            user_id = validate_id(raw_id)
        except ValueError as ve:
            logger.warning("Invalid user id: %s", str(ve))
            return error_response(400, str(ve))

        logger.info("Processing Request for user id: %d", user_id)

        required_params = ["task_external_api"]
        config = fetch_ssm_parameters(required_params)
        posts, comments = asyncio.get_event_loop().run_until_complete(
            fetch_posts_comments(user_id, config["task_external_api"])
        )

        if not posts:
            logger.info("No posts found for this user: %d", user_id)
            return success_response(
                {
                    "userId": user_id,
                    "posts": [],
                    "metadata": {"totalPosts": 0, "totalComments": 0},
                }
            )
        hierarchical_data = build_hierarchical_data(user_id, posts, comments)
        return success_response(hierarchical_data)
    except aiohttp.ClientError as ce:
        logger.error("External API Error: %s", str(ce))
        return error_response(502, "Failed to fetch data from external API")
    except asyncio.TimeoutError as te:
        logger.error("External API Error: %s", str(te))
        return error_response(504, "API response time out")
    except Exception as e:
        logger.error("Unexpected error: %s", str(e))
        return error_response(500, "Internal Server Error")
