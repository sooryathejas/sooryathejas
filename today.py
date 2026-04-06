import datetime
import hashlib
import os
import re
import time
from pathlib import Path

import requests
from dateutil import relativedelta
from dotenv import load_dotenv
from lxml.etree import parse

load_dotenv()

GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
CACHE_DIR = Path("cache")
ARCHIVE_PATH = CACHE_DIR / "repository_archive.txt"
SVG_FILES = ("dark_mode.svg", "light_mode.svg")

COMMENT_BLOCK_SIZE = 7
BIRTHDAY = datetime.datetime(2005, 4, 12)
ARCHIVE_USER_ID = ""
CACHE_COMMENT_LINE = "This line is a comment block. Write whatever you want here.\n"

AGE_DATA_WIDTH = 49
COMMIT_DATA_WIDTH = 22
LOC_DATA_WIDTH = 25
FOLLOWER_DATA_WIDTH = 10
REPO_DATA_WIDTH = 6
STAR_DATA_WIDTH = 14
STATS_SECONDARY_COLUMN_WIDTH = 34
STATS_SECONDARY_SEPARATOR = " |  "

QUERY_COUNT = {
    "user_getter": 0,
    "follower_getter": 0,
    "graph_repos_stars": 0,
    "recursive_loc": 0,
    "loc_query": 0,
}

HEADERS = {}
USER_NAME = ""
OWNER_ID = None


def require_env(name):
    value = os.getenv(name)
    if value:
        return value
    raise RuntimeError(f"Missing required environment variable: {name}")


def configure_environment():
    global HEADERS, USER_NAME
    access_token = require_env("ACCESS_TOKEN")
    USER_NAME = require_env("USER_NAME")
    HEADERS = {"authorization": f"token {access_token}"}


def cache_file_path():
    hashed_user = hashlib.sha256(USER_NAME.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{hashed_user}.txt"


def format_age(birthday):
    diff = relativedelta.relativedelta(datetime.datetime.today(), birthday)
    parts = [
        f"{diff.years} year{format_plural(diff.years)}",
        f"{diff.months} month{format_plural(diff.months)}",
        f"{diff.days} day{format_plural(diff.days)}",
    ]
    suffix = " 🎂" if diff.months == 0 and diff.days == 0 else ""
    return ", ".join(parts) + suffix


def format_plural(value):
    return "s" if value != 1 else ""


def raise_request_error(operation_name, response):
    if response.status_code == 403:
        raise RuntimeError("Too many requests. GitHub returned 403.")
    raise RuntimeError(
        f"{operation_name} failed with status {response.status_code}: "
        f"{response.text}. Query counts: {QUERY_COUNT}"
    )


def graphql_request(operation_name, query, variables, partial_cache=None):
    try:
        response = requests.post(
            GITHUB_GRAPHQL_URL,
            json={"query": query, "variables": variables},
            headers=HEADERS,
            timeout=30,
        )
    except requests.RequestException as error:
        if partial_cache is not None:
            force_close_file(*partial_cache)
        raise RuntimeError(f"{operation_name} request failed: {error}") from error

    if response.status_code != 200:
        if partial_cache is not None:
            force_close_file(*partial_cache)
        raise_request_error(operation_name, response)

    try:
        payload = response.json()
    except ValueError as error:
        if partial_cache is not None:
            force_close_file(*partial_cache)
        raise RuntimeError(f"{operation_name} returned invalid JSON: {response.text}") from error

    if payload.get("errors"):
        if partial_cache is not None:
            force_close_file(*partial_cache)
        raise RuntimeError(f"{operation_name} returned GraphQL errors: {payload['errors']}")

    return payload["data"]


def graph_repos_stars(count_type, owner_affiliation):
    total_repositories = 0
    total_stars = 0
    cursor = None

    query = """
    query ($owner_affiliation: [RepositoryAffiliation], $login: String!, $cursor: String) {
        user(login: $login) {
            repositories(first: 100, after: $cursor, ownerAffiliations: $owner_affiliation) {
                totalCount
                edges {
                    node {
                        ... on Repository {
                            stargazers {
                                totalCount
                            }
                        }
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
    }"""

    while True:
        query_count("graph_repos_stars")
        variables = {
            "owner_affiliation": owner_affiliation,
            "login": USER_NAME,
            "cursor": cursor,
        }
        data = graphql_request("graph_repos_stars", query, variables)
        repositories = data["user"]["repositories"]
        total_repositories = repositories["totalCount"]
        total_stars += stars_counter(repositories["edges"])

        if not repositories["pageInfo"]["hasNextPage"]:
            break
        cursor = repositories["pageInfo"]["endCursor"]

    if count_type == "repos":
        return total_repositories
    if count_type == "stars":
        return total_stars
    return 0


def stars_counter(data):
    total = 0
    for repo in data:
        total += repo["node"]["stargazers"]["totalCount"]
    return total


def recursive_loc(owner, repo_name, cache_rows, cache_header,
                  addition_total=0, deletion_total=0, my_commits=0, cursor=None):
    query_count("recursive_loc")
    query = """
    query ($repo_name: String!, $owner: String!, $cursor: String) {
        repository(name: $repo_name, owner: $owner) {
            defaultBranchRef {
                target {
                    ... on Commit {
                        history(first: 100, after: $cursor) {
                            edges {
                                node {
                                    ... on Commit {
                                        author { user { id } }
                                        deletions
                                        additions
                                    }
                                }
                            }
                            pageInfo { endCursor hasNextPage }
                        }
                    }
                }
            }
        }
    }"""
    variables = {"repo_name": repo_name, "owner": owner, "cursor": cursor}
    data = graphql_request("recursive_loc", query, variables,
                           partial_cache=(cache_rows, cache_header, addition_total,
                                          deletion_total, my_commits))
    if data["repository"]["defaultBranchRef"] is None:
        return addition_total, deletion_total, my_commits

    history = data["repository"]["defaultBranchRef"]["target"]["history"]
    for edge in history["edges"]:
        node = edge["node"]
        if node["author"]["user"] and node["author"]["user"]["id"] == OWNER_ID:
            my_commits += 1
            addition_total += node["additions"]
            deletion_total += node["deletions"]

    if history["pageInfo"]["hasNextPage"]:
        return recursive_loc(owner, repo_name, cache_rows, cache_header,
                             addition_total, deletion_total, my_commits,
                             history["pageInfo"]["endCursor"])
    return addition_total, deletion_total, my_commits


def loc_query(owner_affiliation, comment_size):
    query_count("loc_query")
    filename = cache_file_path()
    cache_rows = []
    cache_header = []

    CACHE_DIR.mkdir(exist_ok=True)
    if filename.exists():
        with filename.open("r") as f:
            data = f.readlines()
        cache_header = data[:comment_size]
        cache_rows = data[comment_size:]
    else:
        cache_header = [CACHE_COMMENT_LINE] * comment_size

    query = """
    query ($owner_affiliation: [RepositoryAffiliation], $login: String!, $cursor: String) {
        user(login: $login) {
            repositories(first: 100, after: $cursor, ownerAffiliations: $owner_affiliation) {
                edges { node { ... on Repository { name owner { login } } } }
                pageInfo { endCursor hasNextPage }
            }
        }
    }"""

    cursor = None
    repos = []
    while True:
        variables = {"owner_affiliation": owner_affiliation, "login": USER_NAME, "cursor": cursor}
        data = graphql_request("loc_query", query, variables)
        repos += data["user"]["repositories"]["edges"]
        if not data["user"]["repositories"]["pageInfo"]["hasNextPage"]:
            break
        cursor = data["user"]["repositories"]["pageInfo"]["endCursor"]

    addition_total = 0
    deletion_total = 0
    my_commits_total = 0
    cached = False

    for repo in repos:
        node = repo["node"]
        repo_id = f"{node['owner']['login']}/{node['name']}"
        found = False
        for row in cache_rows:
            if row.startswith(repo_id + " "):
                parts = row.split()
                addition_total += int(parts[1])
                deletion_total += int(parts[2])
                my_commits_total += int(parts[2])
                found = True
                cached = True
                break
        if not found:
            add, delete, commits = recursive_loc(
                node["owner"]["login"], node["name"], cache_rows, cache_header)
            addition_total += add
            deletion_total += delete
            my_commits_total += commits
            cache_rows.append(f"{repo_id} {add} {delete} {commits}\n")

    with filename.open("w") as f:
        f.writelines(cache_header)
        f.writelines(cache_rows)

    return [addition_total, deletion_total, my_commits_total, cached]


def force_close_file(cache_rows, cache_header, addition_total, deletion_total, my_commits):
    filename = cache_file_path()
    with filename.open("w") as f:
        f.writelines(cache_header)
        f.writelines(cache_rows)


def svg_overwrite(filename, age_data, commit_data, star_data, repo_data,
                  contrib_data, follower_data, loc_data):
    tree = parse(filename)
    root = tree.getroot()

    justify_format(root, "age",       age_data,                    AGE_DATA_WIDTH)
    justify_format(root, "commits",   commit_data,                  COMMIT_DATA_WIDTH)
    justify_format(root, "stars",     star_data,                    STAR_DATA_WIDTH)
    justify_format(root, "repos",     repo_data,                    REPO_DATA_WIDTH)
    justify_format(root, "contrib",   contrib_data,                 REPO_DATA_WIDTH)
    justify_format(root, "followers", follower_data,                FOLLOWER_DATA_WIDTH)

    if isinstance(loc_data, list) and len(loc_data) >= 3:
        loc_str = f"+{loc_data[0]}, -{loc_data[1]}"
        justify_format(root, "loc",   loc_str,                      LOC_DATA_WIDTH)

    tree.write(filename, xml_declaration=True, encoding="utf-8", pretty_print=True)


def justify_format(root, element_id, new_text, length=0):
    new_text = format_display_text(new_text)
    find_and_replace(root, element_id, new_text)
    dot_string = build_dot_string(new_text, length)
    find_and_replace(root, f"{element_id}_dots", dot_string)


def format_display_text(value):
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def build_dot_string(value_text, length):
    just_len = max(0, length - len(value_text))
    if just_len <= 2:
        dot_map = {0: "", 1: " ", 2: ". "}
        return dot_map[just_len]
    return " " + ("." * just_len) + " "


def secondary_stat_gap(left_width, target_width=STATS_SECONDARY_COLUMN_WIDTH):
    return (" " * max(0, target_width - left_width)) + STATS_SECONDARY_SEPARATOR


def find_and_replace(root, element_id, new_text):
    element = root.find(f".//*[@id='{element_id}']")
    if element is not None:
        element.text = new_text


def format_compact_number(value):
    if isinstance(value, str):
        normalized = value.replace(",", "").strip().upper()
        if normalized.endswith("M"):
            return value
        if normalized.endswith("K"):
            return value
        value = int(normalized)
    absolute_value = abs(value)
    if absolute_value >= 1_000_000:
        formatted = f"{value / 1_000_000:.2f}".rstrip("0").rstrip(".")
        return f"{formatted}M"
    if absolute_value >= 1_000:
        formatted = f"{value / 1_000:.1f}".rstrip("0").rstrip(".")
        return f"{formatted}K"
    return str(value)


def commit_counter(comment_size):
    total_commits = 0
    filename = cache_file_path()
    if not filename.exists():
        return 0
    with filename.open("r") as handle:
        data = handle.readlines()
    for line in data[comment_size:]:
        parts = line.split()
        if len(parts) >= 3:
            total_commits += int(parts[2])
    return total_commits


def user_getter(username):
    query_count("user_getter")
    query = """
    query($login: String!){
        user(login: $login) { id }
    }"""
    data = graphql_request("user_getter", query, {"login": username})
    return data["user"]["id"]


def follower_getter(username):
    query_count("follower_getter")
    query = """
    query($login: String!){
        user(login: $login) {
            followers { totalCount }
        }
    }"""
    data = graphql_request("follower_getter", query, {"login": username})
    return int(data["user"]["followers"]["totalCount"])


def query_count(function_name):
    QUERY_COUNT[function_name] += 1


def perf_counter(function, *args):
    start = time.perf_counter()
    result = function(*args)
    return result, time.perf_counter() - start


def print_duration(label, duration):
    metric = f"{duration:.4f} s" if duration > 1 else f"{duration * 1000:.4f} ms"
    print(f"   {label + ':':<20}{metric:>12}")


def update_svg_files(age_data, commit_data, star_data, repo_data,
                     contrib_data, follower_data, loc_data):
    for svg_file in SVG_FILES:
        svg_overwrite(svg_file, age_data, commit_data, star_data,
                      repo_data, contrib_data, follower_data, loc_data)


def main():
    global OWNER_ID

    configure_environment()
    print("Calculation times:")

    OWNER_ID, user_time = perf_counter(user_getter, USER_NAME)
    print_duration("account data", user_time)

    age_data, age_time = perf_counter(format_age, BIRTHDAY)
    print_duration("age calculation", age_time)

    total_loc, loc_time = perf_counter(
        loc_query,
        ["OWNER", "COLLABORATOR", "ORGANIZATION_MEMBER"],
        COMMENT_BLOCK_SIZE,
    )
    print_duration("LOC (cached)" if total_loc[-1] else "LOC (no cache)", loc_time)

    commit_data, commit_time = perf_counter(commit_counter, COMMENT_BLOCK_SIZE)
    print_duration("commit count", commit_time)

    star_data, star_time = perf_counter(graph_repos_stars, "stars", ["OWNER"])
    print_duration("stars", star_time)

    repo_data, repo_time = perf_counter(graph_repos_stars, "repos", ["OWNER"])
    print_duration("repos", repo_time)

    contrib_data, contrib_time = perf_counter(
        graph_repos_stars, "repos",
        ["OWNER", "COLLABORATOR", "ORGANIZATION_MEMBER"],
    )
    print_duration("contributed repos", contrib_time)

    follower_data, follower_time = perf_counter(follower_getter, USER_NAME)
    print_duration("followers", follower_time)

    total_loc[:-1] = [f"{value:,}" for value in total_loc[:-1]]

    update_svg_files(age_data, commit_data, star_data, repo_data,
                     contrib_data, follower_data, total_loc[:-1])

    total_runtime = (user_time + age_time + loc_time + commit_time +
                     star_time + repo_time + contrib_time + follower_time)
    print(f"{'Total function time:':<21} {total_runtime:>11.4f} s")
    print(f"Total GitHub GraphQL API calls: {sum(QUERY_COUNT.values()):>3}")
    for function_name, count in QUERY_COUNT.items():
        print(f"   {function_name + ':':<25} {count:>6}")


if __name__ == "__main__":
    main()
