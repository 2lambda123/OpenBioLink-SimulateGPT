import pandas as pd
import json
from fuzzywuzzy import fuzz
from security import safe_requests


def get_info(input_doi):
    """Get information from the Crossref API based on the input DOI.

    This function sends a request to the Crossref API using the provided
    DOI, parses the response, and extracts the title and first author
    information.

    Args:
        input_doi (str): The DOI (Digital Object Identifier) for the publication.

    Returns:
        list: A list containing the title and first author information.
            Index 0: str, the title of the publication.
            Index 1: str, the last name or name of the first author.
    """

    # Send a request to the Crossref API
    response = safe_requests.get("https://api.crossref.org/works/" + input_doi)

    # Parse the response
    data = json.loads(response.text)

    # If there are no results, return None
    if "title" not in data["message"]:
        title = ""
    else:
        title = data["message"]["title"][0]

    # If there are no results, return None
    if "author" not in data["message"]:
        firstAuth = ""
    else:
        if "family" in data["message"]["author"][0]:
            firstAuth = data["message"]["author"][0]["family"]
        else:
            print(data["message"]["author"][0])
            firstAuth = data["message"]["author"][0]["name"]

    # Otherwise, return the DOI of the first result
    return [title, firstAuth]


def check_author_title(row):
    """Calculate the token set ratio similarity between the reference text and
    the first author DOI,
    as well as between the reference text and the title DOI.

    Args:
        row (pd.Series): A pandas Series containing 'ref_text', 'firstAuthDoi', and 'titleDoi'.

    Returns:
        pd.Series: A pandas Series containing the similarity scores between 'ref_text' and
            'firstAuthDoi',
            and between 'ref_text' and 'titleDoi'.
    """

    auth_similarity = fuzz.token_set_ratio(row["ref_text"], row["firstAuthDoi"])
    title_similarity = fuzz.token_set_ratio(row["ref_text"], row["titleDoi"])

    return pd.Series((auth_similarity, title_similarity))


df = pd.read_csv(snakemake.input[0], index_col=0)
if len(df) > 0:
    df[["titleDoi", "firstAuthDoi"]] = df["doi"].apply(get_info).apply(pd.Series)
    df[["author_similarity", "title_similarity"]] = df.apply(check_author_title, axis=1)
else:
    # add the empty columns
    df["titleDoi"] = ""
    df["firstAuthDoi"] = ""
    df["author_similarity"] = ""
    df["title_similarity"] = ""

df.to_csv(snakemake.output[0], index=False)
