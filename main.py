from dotenv import load_dotenv
from pathlib import Path
import os
import requests
from requests.auth import HTTPBasicAuth
from typing import List
import csv
import time
from collections import deque

# Load environment variables from a .env file.
load_dotenv()

# Constants for API key, URL, and credentials.
API_KEY = os.getenv("API_KEY")
API_URL = os.getenv("API_URL")

if not API_KEY or not API_URL:
    raise ValueError("API_KEY or API_URL is not set in the environment variables.")

auth = HTTPBasicAuth(API_KEY, "")

# Constants for rate limiting.
MAX_REQUESTS_PER_MINUTE = 500
REQUESTS_TIMES_QUEUE = deque()

def pace_requests():
    """
    Ensures requests are paced under the threshold of MAX_REQUESTS_PER_MINUTE.
    Calculates wait time to maintain the rate under the threshold.
    """
    global REQUESTS_TIMES_QUEUE
    current_time = time.time()

    # Remove timestamps older than the last minute from the queue.
    while REQUESTS_TIMES_QUEUE and REQUESTS_TIMES_QUEUE[0] < current_time - 60:
        REQUESTS_TIMES_QUEUE.popleft()

    # Check if the request count in the last minute exceeds the limit.
    if len(REQUESTS_TIMES_QUEUE) >= MAX_REQUESTS_PER_MINUTE:
        wait_time = 60 - (current_time - REQUESTS_TIMES_QUEUE[0])  # Time until we're under the limit
        print(f"Rate limit reached (500 requests per minute). Waiting for {wait_time} seconds...")
        time.sleep(wait_time)

    # Add the current request timestamp to the queue.
    REQUESTS_TIMES_QUEUE.append(current_time)

def handle_rate_limiting(response: requests.Response) -> None:
    """
    Handles rate-limiting based on the response headers.
    Waits if 'x-ratelimit-remaining' is below 10 or if HTTP 429 is received.
    """
    remaining_limit = int(response.headers.get("x-ratelimit-remaining", "100"))  # Default to 100 if header is missing.

    # Warn and wait if rate limit remaining is low.
    if remaining_limit < 10:
        print(f"Rate limit is low: {remaining_limit} remaining. Waiting for 30 seconds...")
        time.sleep(30)
    else:
        print(f"Remaining rate limit requests: {remaining_limit}.")

    # Handle rate-limiting exceeded (HTTP 429).
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", "30"))  # Default to 30 seconds if header is missing.
        print(f"Rate limit exceeded. Waiting for {retry_after} seconds...")
        time.sleep(retry_after)

def make_request_with_rate_limit(method: str, url: str, **kwargs) -> requests.Response:
    """
    Makes an HTTP request while handling rate-limiting conditions and pacing requests to under 500 per minute.

    Retries the request after waiting if the API responds with HTTP 429 (rate limit exceeded).

    Args:
        method (str): HTTP method ('GET', 'POST', 'PUT', etc.).
        url (str): URL to send the request to.
        **kwargs: Additional arguments for the `requests.request` method.

    Returns:
        requests.Response: The HTTP response.
    """
    global REQUESTS_TIMES_QUEUE

    while True:
        try:
            # Apply pacing to ensure requests do not exceed the rate limit.
            pace_requests()

            # Make the request.
            response = requests.request(method, url, **kwargs)

            # Handle rate-limiting.
            handle_rate_limiting(response)

            # If the status is not 429 (rate limit exceeded), return the response.
            if response.status_code != 429:
                return response
        except requests.RequestException as e:
            print(f"Error making request to {url}: {e}")
            time.sleep(5)  # Wait briefly before retrying.


def deactivate_requester(requester_id: int) -> None:
    """Makes a DELETE request to deactivate a requester by ID."""
    print(f"Deactivating requester {requester_id}...")
    response = make_request_with_rate_limit("DELETE", f"{API_URL}/requesters/{requester_id}", auth=auth)

    if response.status_code == 204:
        print(f"Successfully deactivated requester {requester_id}.")
    elif response.status_code == 404:
        print(f"Requester {requester_id} not found.")
    elif response.status_code == 405:
        try:
            response_body = response.json()
            if response_body.get("message") == "DELETE method is not allowed. It should be one of these method(s): GET":
                print(f"Requester {requester_id} is already deactivated.")
            else:
                print(f"Received HTTP 405 but message: {response_body.get('message')}.")
        except ValueError:
            print(f"Received HTTP 405 but failed to parse the response body.")
    else:
        print(
            f"Failed to deactivate requester {requester_id}. HTTP Status: {response.status_code}, Response: {response.text}")

def reactivate_requester(requester_id: int) -> None:
    """Makes a POST request to activate a requester by ID."""
    print(f"Reactivating requester {requester_id}...")

    # Set the header to include Content-Type: application/json
    headers = {
        "Content-Type": "application/json"
    }

    response = make_request_with_rate_limit(
        "PUT",
        f"{API_URL}/requesters/{requester_id}/reactivate",
        auth=auth,
        headers=headers
    )

    if response.status_code == 200:
        print(f"Successfully reactivated requester {requester_id}.")

    elif response.status_code == 404:
        print(f"Received 404 error for requester {requester_id}. Checking if they exist and are already active...")

        # Perform a GET request to check the requester status.
        get_response = make_request_with_rate_limit(
            "GET",
            f"{API_URL}/requesters/{requester_id}",
            auth=auth
        )

        if get_response.status_code == 200:
            try:
                response_data = get_response.json()
                requester_active = response_data.get("requester", {}).get("active", False)
                if requester_active:
                    print(f"Requester {requester_id} exists and is already active. No further action is needed.")
                else:
                    print(
                        f"Requester {requester_id} exists but is not active. Please verify their status in the system.")
            except ValueError:
                print(f"Failed to parse response for requester {requester_id}. Response: {get_response.text}")
        elif get_response.status_code == 404:
            print(f"Requester {requester_id} does not exist.")
        else:
            print(
                f"Failed to get status for requester {requester_id}. HTTP Status: {get_response.status_code}, Response: {get_response.text}")

    else:
        print(
            f"Failed to reactivate requester {requester_id}. HTTP Status: {response.status_code}, Response: {response.text}")

def merge_requesters(file_path: Path) -> None:
    """
    Reads a CSV file containing primary requester IDs and a single secondary requester ID.
    Makes a PUT request to merge the secondary requester into the primary requester.
    Reactivates the primary requester if the response indicates they should be active (HTTP 400 with specific code).
    Handles primary or secondary requester not found error (HTTP 404).
    """
    print(f"Reading data from CSV file: {file_path}")
    try:
        with file_path.open() as csvfile:
            csv_reader = csv.reader(csvfile)
            for row in csv_reader:
                if len(row) < 2:
                    print(f"Invalid row format: {row}")
                    continue

                # Parse the primary requester ID and the single secondary requester ID
                primary_requester_id = int(row[0])
                secondary_requester_id = int(row[1])  # Expect exactly one secondary requester

                print(
                    f"Merging secondary requester {secondary_requester_id} into primary requester {primary_requester_id}..."
                )

                # Set the header to include Content-Type: application/json
                headers = {
                    "Content-Type": "application/json"
                }

                # Construct the query string with the single secondary requester ID
                query_params = {"secondary_requesters": secondary_requester_id}
                response = make_request_with_rate_limit(
                    "PUT",
                    f"{API_URL}/requesters/{primary_requester_id}/merge",
                    params=query_params,
                    auth=auth,
                    headers=headers
                )

                if response.status_code in (200, 204):
                    print(
                        f"Successfully merged secondary requester {secondary_requester_id} into primary requester {primary_requester_id}."
                    )
                elif response.status_code == 400:
                    response_json = response.json()
                    if response_json.get("code") == "primary_requester_should_be_active":
                        print(f"Primary requester {primary_requester_id} is inactive. Attempting to reactivate...")
                        reactivate_requester(primary_requester_id)
                        print(f"Retrying the merge operation for primary requester {primary_requester_id}...")
                        retry_response = make_request_with_rate_limit(
                            "PUT",
                            f"{API_URL}/requesters/{primary_requester_id}/merge",
                            params=query_params,
                            auth=auth,
                            headers=headers
                        )
                        if retry_response.status_code in (200, 204):
                            print(
                                f"Successfully merged secondary requester {secondary_requester_id} into primary requester {primary_requester_id} after reactivating."
                            )
                            print(f"Deactivating primary requester {primary_requester_id} again...")
                            deactivate_requester(primary_requester_id)
                        else:
                            print(
                                f"Failed to merge secondary requester {secondary_requester_id} into primary requester {primary_requester_id} after reactivating. HTTP Status: {retry_response.status_code}, Response: {retry_response.text}"
                            )
                    else:
                        print(
                            f"Failed to merge secondary requester. HTTP Status: {response.status_code}, Response: {response.text}"
                        )
                elif response.status_code == 404:
                    print(
                        f"Error: HTTP Status 404. Primary requester {primary_requester_id} or secondary requester {secondary_requester_id} was not found or is deactivated."
                    )
                else:
                    print(
                        f"Failed to merge secondary requester {secondary_requester_id} into primary requester {primary_requester_id}. HTTP Status: {response.status_code}, Response: {response.text}"
                    )

    except FileNotFoundError as e:
        print(f"Error: File {file_path} not found. {e}")
    except ValueError as e:
        print(f"Error: Invalid data in CSV file. Each row must contain exactly two integers: primary requester ID and secondary requester ID. Details: {e}")


def update_requester_emails(file_path: Path) -> None:
    """
    Reads a CSV file containing requester IDs, new primary emails, and new secondary emails.
    Updates requester emails by performing two API calls:
    1. Clears all secondary emails and sets the primary email.
    2. Sets the secondary email.

    Args:
        file_path (Path): Path to the CSV file. Defaults to "update_requester_emails.csv".
    """
    print(f"Reading data from CSV file: {file_path}")

    try:
        # Open the CSV file and read its rows
        with file_path.open() as csvfile:
            csv_reader = csv.reader(csvfile)

            for row in csv_reader:
                if len(row) < 3:
                    print(f"Invalid row format: {row}. Skipping...")
                    continue  # Ensure every row has valid data (requester_id, primary email, secondary email)

                try:
                    requester_id = int(row[0])  # Parse requester ID as an integer
                    new_primary_email = row[1].strip()  # Incoming primary email from CSV
                    new_secondary_email = row[2].strip()  # Incoming secondary email from CSV

                    # First API call: Clear all secondary emails and set the primary email
                    print(f"Updating primary email and clearing secondary emails for requester {requester_id}...")
                    clear_secondary_emails_body = {
                        "primary_email": new_primary_email,
                        "secondary_emails": []
                    }

                    response1 = make_request_with_rate_limit(
                        "PUT",
                        f"{API_URL}/requesters/{requester_id}",
                        json=clear_secondary_emails_body,
                        headers={"Content-Type": "application/json"},
                        auth=auth
                    )

                    if response1.status_code == 200:
                        print(
                            f"Successfully updated primary email for requester {requester_id} and cleared secondary emails.")
                    else:
                        print(
                            f"Failed to update primary email for requester {requester_id}. HTTP Status: {response1.status_code}, Response: {response1.text}")
                        continue  # Skip further processing for this requester if the first API call fails

                    # Second API call: Set the new secondary email
                    print(f"Updating secondary email for requester {requester_id}...")
                    update_secondary_emails_body = {
                        "secondary_emails": [new_secondary_email]
                    }

                    response2 = make_request_with_rate_limit(
                        "PUT",
                        f"{API_URL}/requesters/{requester_id}",
                        json=update_secondary_emails_body,
                        headers={"Content-Type": "application/json"},
                        auth=auth
                    )

                    if response2.status_code == 200:
                        print(f"Successfully updated secondary email for requester {requester_id}.")
                    else:
                        print(
                            f"Failed to update secondary email for requester {requester_id}. HTTP Status: {response2.status_code}, Response: {response2.text}")

                except ValueError as e:
                    print(f"Invalid requester ID format in row: {row}. Skipping... Details: {e}")
                except Exception as e:
                    print(f"An unexpected error occurred while processing row: {row}. Details: {e}")

    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
    except Exception as e:
        print(f"An unexpected error occurred. Details: {e}")

def load_requester_ids(file_path: Path) -> List[int]:
    """Reads a text file and loads requester IDs."""
    try:
        with file_path.open() as file:
            return [int(line.strip()) for line in file if line.strip().isdigit()]
    except FileNotFoundError as e:
        print(f"Error: File {file_path} not found. {e}")
        return []
    except ValueError as e:
        print(f"Error: Invalid data in file {file_path}. {e}")
        return []


def add_secondary_emails(file_path: Path) -> None:
    """
    Reads a CSV file containing requester IDs and one or more columns for secondary emails to add.
    Adds the provided secondary emails to the array of existing secondary emails using two API calls:
    1. HTTP GET to fetch the current array of secondary emails.
    2. HTTP PUT to update the secondary emails array.

    Args:
        file_path (Path): Path to the CSV file containing requester IDs and secondary emails to add.
    """
    print(f"Reading data from CSV file: {file_path}")

    try:
        # Open the CSV file and read its rows
        with file_path.open() as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)  # Read the header row

            # Ensure the CSV file has required columns (minimum: requester_id, and 1 or more secondary email columns)
            if len(header) < 2:
                print("Invalid CSV format. The file must include at least two columns: requester_id and secondary emails.")
                return

            for row in csv_reader:
                if len(row) < 2:
                    print(f"Invalid row format: {row}. Skipping...")
                    continue  # Skip rows with insufficient columns

                try:
                    # Extract requester ID and secondary emails from the row
                    requester_id = int(row[0])
                    secondary_emails_to_add = [email.strip() for email in row[1:] if email.strip()]  # Remove empty/invalid emails

                    # If no secondary emails to add, skip this requester
                    if not secondary_emails_to_add:
                        print(f"No secondary emails to add for requester {requester_id}. Skipping...")
                        continue

                    # Step 1: HTTP GET to fetch current secondary emails
                    print(f"Fetching existing secondary emails for requester {requester_id}...")
                    get_response = make_request_with_rate_limit(
                        "GET",
                        f"{API_URL}/requesters/{requester_id}",
                        auth=auth
                    )

                    if get_response.status_code == 200:
                        existing_secondary_emails = get_response.json().get("requester", {}).get("secondary_emails", [])
                        if not isinstance(existing_secondary_emails, list):
                            print(f"Unexpected format for existing secondary emails for requester {requester_id}. Skipping...")
                            continue

                        print(f"Existing secondary emails for requester {requester_id}: {existing_secondary_emails}")

                        # Combine existing emails with the emails to add from the CSV, ensuring there are no duplicates
                        new_secondary_emails = list(set(existing_secondary_emails + secondary_emails_to_add))
                        print(f"New secondary emails for requester {requester_id}: {new_secondary_emails}")

                        # Step 2: HTTP PUT to update the secondary emails
                        print(f"Setting new secondary emails for requester {requester_id}...")
                        update_body = {
                            "secondary_emails": new_secondary_emails
                        }

                        put_response = make_request_with_rate_limit(
                            "PUT",
                            f"{API_URL}/requesters/{requester_id}",
                            json=update_body,
                            headers={"Content-Type": "application/json"},
                            auth=auth
                        )

                        if put_response.status_code == 200:
                            print(f"Successfully updated secondary emails for requester {requester_id}.")
                        else:
                            print(f"Failed to update secondary emails for requester {requester_id}. HTTP Status: {put_response.status_code}, Response: {put_response.text}")
                    elif get_response.status_code == 404:
                        print(f"Requester {requester_id} not found. Skipping...")
                    else:
                        print(f"Failed to fetch secondary emails for requester {requester_id}. HTTP Status: {get_response.status_code}, Response: {get_response.text}")

                except ValueError as e:
                    print(f"Invalid requester ID format in row: {row}. Skipping... Details: {e}")
                except Exception as e:
                    print(f"An unexpected error occurred while processing row: {row}. Details: {e}")

    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
    except Exception as e:
        print(f"An unexpected error occurred. Details: {e}")

def update_requester_external_id(file_path: Path) -> None:
    """
    Reads a CSV file containing requester IDs and external IDs, and updates the external_id
    for each requester using an HTTP PUT request.

    Args:
        file_path (Path): Path to the CSV file containing requester IDs and external IDs.
    """
    print(f"Reading data from CSV file: {file_path}")

    try:
        # Open the CSV file and read its rows
        with file_path.open() as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)  # Read the header row

            # Ensure the CSV file has required columns: requester_id, external_id
            if header != ["requester_id", "external_id"]:
                print(f"Invalid CSV format. Expected columns: requester_id, external_id.")
                return

            for row in csv_reader:
                if len(row) < 2:
                    print(f"Invalid row format: {row}. Skipping...")
                    continue  # Skip rows with insufficient columns

                try:
                    requester_id = int(row[0])  # Parse requester ID as an integer
                    external_id = row[1].strip()  # Get the external_id as a string

                    # Make the PUT request to update the external_id
                    print(f"Updating external_id for requester {requester_id}...")
                    update_body = {
                        "external_id": external_id
                    }

                    response = make_request_with_rate_limit(
                        "PUT",
                        f"{API_URL}/requesters/{requester_id}",
                        json=update_body,
                        headers={"Content-Type": "application/json"},
                        auth=auth
                    )

                    if response.status_code == 200:
                        print(f"Successfully updated external_id for requester {requester_id} to '{external_id}'.")
                    elif response.status_code == 404:
                        print(f"Requester {requester_id} not found. Skipping...")
                    else:
                        print(
                            f"Failed to update external_id for requester {requester_id}. HTTP Status: {response.status_code}, Response: {response.text}")

                except ValueError as e:
                    print(f"Invalid requester ID format in row: {row}. Skipping... Details: {e}")
                except Exception as e:
                    print(f"An unexpected error occurred while processing row: {row}. Details: {e}")

    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
    except Exception as e:
        print(f"An unexpected error occurred. Details: {e}")

if __name__ == "__main__":
    # Ask the user which action they want to perform.
    action = input("Enter action ('deactivate', 'reactivate', 'merge', 'update_requester_emails', 'add_secondary_emails', or 'update_external_id'): ").strip().lower()

    if action not in ("deactivate", "reactivate", "merge", "update_requester_emails", "add_secondary_emails", "update_external_id"):
        print("Invalid action. Please enter 'deactivate', 'reactivate', 'merge', 'update_requester_emails', 'add_secondary_emails', or 'update_external_id'.")
        exit(1)

    if action in ("deactivate", "reactivate"):
        # Ask for the path to the requester IDs file or use the default 'requester_ids.txt'.
        requester_ids_path = input("Enter the path to the requester IDs file (default: requester_ids.txt): ").strip()
        if not requester_ids_path:
            requester_ids_path = "requester_ids.txt"
        requester_ids_file = Path(requester_ids_path)

        # Verify the file exists.
        if not requester_ids_file.exists():
            print(f"Error: File {requester_ids_file} does not exist.")
            exit(1)

        # Load requester IDs.
        requester_ids = load_requester_ids(requester_ids_file)

        if not requester_ids:
            print("No valid requester IDs found in the file.")
            exit(1)

        for requester_id in requester_ids:
            if action == "deactivate":
                deactivate_requester(requester_id)
            elif action == "reactivate":
                reactivate_requester(requester_id)

    elif action == "merge":
        # Ask for the path to the CSV file or use the default 'merge.csv'.
        merge_csv_path = input("Enter the path to the CSV file containing merge data (default: merge.csv): ").strip()
        if not merge_csv_path:
            merge_csv_path = "merge.csv"
        merge_csv_file = Path(merge_csv_path)

        # Verify the file exists.
        if not merge_csv_file.exists():
            print(f"Error: File {merge_csv_file} does not exist.")
            exit(1)

        # Perform the merge action.
        merge_requesters(merge_csv_file)

    elif action == "update_requester_emails":
        # Ask for the path to the CSV file or use the default 'update_requester_emails.csv'.
        update_emails_csv_path = input("Enter the path to the CSV file containing email update data (default: update_requester_emails.csv): ").strip()
        if not update_emails_csv_path:
            update_emails_csv_path = "update_requester_emails.csv"
        update_emails_csv_file = Path(update_emails_csv_path)

        # Verify the file exists.
        if not update_emails_csv_file.exists():
            print(f"Error: File {update_emails_csv_file} does not exist.")
            exit(1)

        # Perform the email update action.
        update_requester_emails(update_emails_csv_file)

    elif action == "add_secondary_emails":
        # Ask for the path to the CSV file or use the default 'add_secondary_emails.csv'.
        add_secondary_emails_csv_path = input("Enter the path to the CSV file containing secondary emails to add (default: add_secondary_emails.csv): ").strip()
        if not add_secondary_emails_csv_path:
            add_secondary_emails_csv_path = "add_secondary_emails.csv"
        add_secondary_emails_csv_file = Path(add_secondary_emails_csv_path)

        # Verify the file exists.
        if not add_secondary_emails_csv_file.exists():
            print(f"Error: File {add_secondary_emails_csv_file} does not exist.")
            exit(1)

        # Perform the add secondary emails action.
        add_secondary_emails(add_secondary_emails_csv_file)

    elif action == "update_external_id":
        # Ask for the path to the CSV file or use the default 'update_requester_external_ids.csv'.
        external_id_csv_path = input("Enter the path to the CSV file containing external IDs to update (default: update_requester_external_ids.csv): ").strip()
        if not external_id_csv_path:
            external_id_csv_path = "update_requester_external_ids.csv"
        external_id_csv_file = Path(external_id_csv_path)

        # Verify the file exists.
        if not external_id_csv_file.exists():
            print(f"Error: File {external_id_csv_file} does not exist.")
            exit(1)

        # Perform the external ID update action.
        update_requester_external_id(external_id_csv_file)