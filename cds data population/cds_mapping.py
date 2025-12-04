#!/usr/bin/env python3
import json
from pathlib import Path

import requests  # pip install requests

BASE_URL = "https://qa-cds.zinnia.com/correspondence/api/sample/Policy"


def normalize_name(s: str) -> str:
    """
    Normalize a variable / field / role name for comparison:
    - Uppercase
    - Strip leading/trailing spaces
    - Remove all spaces and underscores
    """
    return "".join(
        ch for ch in str(s).upper().strip()
        if not ch.isspace() and ch != "_"
    )


def extract_scalar_value(value) -> str:
    """
    Convert a CDS value into a string for output.
    - None -> ""
    - empty list -> ""
    - list of strings -> first string
    - list with no strings -> ""
    - other scalars -> str(value)
    - dict / other -> str(value)
    """
    if value is None:
        return ""

    if isinstance(value, list):
        # For Email or any list: prefer first string
        # If empty list or no strings found, return ""
        if len(value) == 0:
            return ""
        for item in value:
            if isinstance(item, str):
                return item
        # List exists but contains no strings -> return ""
        return ""

    if isinstance(value, (str, int, float, bool)):
        return str(value)

    return str(value)


def lookup_root_field(var_str: str, root_obj: dict):
    """
    Look for a root-level CDS field matching var_str (no underscore variable).
    Matching is case-insensitive and whitespace/underscore-insensitive.
    """
    target = normalize_name(var_str)
    for key, value in root_obj.items():
        if isinstance(key, str) and normalize_name(key) == target:
            return extract_scalar_value(value)
    return None


def find_person_by_role(people_list, role_token: str):
    """
    Find the first person in 'people' whose Role matches role_token.
    Role matching is case-insensitive and whitespace/underscore-insensitive.
    """
    target = normalize_name(role_token)
    for person in people_list:
        if not isinstance(person, dict):
            continue
        role_val = person.get("Role")
        if not isinstance(role_val, str):
            continue
        if normalize_name(role_val) == target:
            return person
    return None


def find_value_in_obj(obj, field_token: str):
    """
    Search for a field with name matching field_token anywhere within obj (DFS).
    obj is a dict/list/scalar inside a single 'person' object.
    Returns the first extracted scalar value found, or None if not found.
    """
    target = normalize_name(field_token)

    def _search(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if isinstance(k, str) and normalize_name(k) == target:
                    return extract_scalar_value(v)
                found = _search(v)
                if found is not None:
                    return found
        elif isinstance(o, list):
            for item in o:
                found = _search(item)
                if found is not None:
                    return found
        # Scalars: nothing deeper to search
        return None

    return _search(obj)


def lookup_people_field(root_obj: dict, role_token: str, var_token: str):
    """
    Given CDS root object, a Role token (e.g. PRIMOWNER) and a VarName token
    (e.g. FIRSTNAME, CITY, ADDRESSLINE1, ROUTINGNUMBER),
    find the matching value inside the appropriate person object.
    """
    people_list = root_obj.get("people")
    if not isinstance(people_list, list):
        return None

    person = find_person_by_role(people_list, role_token)
    if person is None:
        return None

    return find_value_in_obj(person, var_token)


def resolve_variable(var_str: str, root_obj: dict) -> str:
    """
    Resolve a variable string using the CDS root object.

    Rules:
    - PEOPLE_{RoleToken}_{VarNameToken}:
        * locate person by Role, then VarName anywhere within that person.
    - No underscore at all:
        * treat as root-level field, search only at top level of CDS object.
    - Anything else (contains '_' but not PEOPLE_ prefix):
        * treated as unresolved; becomes "".

    All matches are case-insensitive and whitespace-/underscore-insensitive.
    Unresolved variables return "".
    """
    if var_str is None:
        return ""

    var_clean = str(var_str).strip()
    if not var_clean:
        return ""

    var_upper = var_clean.upper()

    # PEOPLE_ role-based variables
    if var_upper.startswith("PEOPLE_"):
        rest = var_upper[len("PEOPLE_"):]  # after PEOPLE_
        role_token, sep, var_token = rest.partition("_")
        if not sep:
            # malformed PEOPLE_ variable (no var token)
            return ""
        value = lookup_people_field(root_obj, role_token, var_token)
        return value if value is not None else ""

    # Root-level variables: no underscore at all
    if "_" not in var_clean:
        value = lookup_root_field(var_clean, root_obj)
        return value if value is not None else ""

    # All other patterns (e.g. M_..., D_...) are considered unresolved
    return ""


def transform_structure(obj, root_obj: dict):
    """
    Recursively walk the input JSON structure and replace every string value
    using resolve_variable(). Non-string types are left as-is.
    """
    if isinstance(obj, dict):
        return {k: transform_structure(v, root_obj) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [transform_structure(item, root_obj) for item in obj]
    elif isinstance(obj, str):
        return resolve_variable(obj, root_obj)
    else:
        # numbers, bools, null, etc. stay unchanged
        return obj


def fetch_cds_root(contract_number: str) -> dict:
    """
    Call the CDS sample/Policy API and return the root object
    (first element of the response list).
    """
    try:
        resp = requests.get(
            BASE_URL,
            params={"contractnumber": contract_number},
            timeout=10,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        raise SystemExit(f"Error calling CDS API: {e}")

    try:
        data = resp.json()
    except ValueError as e:
        raise SystemExit(f"Error parsing CDS API response as JSON: {e}")

    if not isinstance(data, list) or not data or not isinstance(data[0], dict):
        raise SystemExit(
            "Unexpected CDS API response: expected a JSON array with one object."
        )

    return data[0]


def main():
    base_dir = Path(__file__).resolve().parent
    input_dir = base_dir / "input"
    output_dir = base_dir / "output"

    input_path = input_dir / "input.json"
    output_path = output_dir / "output.json"

    if not input_path.exists():
        raise SystemExit(f"Could not find input JSON: {input_path}")

    # Read input template JSON
    with input_path.open("r", encoding="utf-8") as f:
        input_data = json.load(f)

    # Ask for contract number
    contract_number = input("Enter contract number: ").strip()
    if not contract_number:
        raise SystemExit("Contract number cannot be empty.")

    # Fetch CDS JSON for this contract
    root_obj = fetch_cds_root(contract_number)

    # Transform the template using CDS data
    output_data = transform_structure(input_data, root_obj)

    # Ensure output directory exists and write result
    output_dir.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)

    print(f"Filled JSON written to: {output_path}")


if __name__ == "__main__":
    main()
