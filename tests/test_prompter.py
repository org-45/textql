import unittest
import logging
from src.helper.prompter import construct_prompt

logging.basicConfig(level=logging.DEBUG)

class TestConstructPrompt(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None  

    def test_construct_prompt_basic(self):
        logging.debug("Starting test_construct_prompt_basic")
        natural_language_input = "Get all users"
        top_k = "Result1"
        queries = [
            {"description": "Fetch active users", "sql": "SELECT * FROM users WHERE status = 'active'"}
        ]
        schema = {
            "tables": {
                "users": {
                    "columns": ["id", "name", "email", "status"],
                }
            }
        }

        expected_prompt = f"""
    Act as a data analyst and SQL expert. Translate this natural language input into a SQL query for a Postgres DB:
    "{natural_language_input}"

    Schema:
    Table: users,Columns: id, name, email, status
    
    Reference examples:
    - Fetch active users: SELECT * FROM users WHERE status = 'active'

    Similar results from vector search:
    Result1

    Ensure the query is safe (no DROP, DELETE, UPDATE) and correct typos based on schema.
    Return only the SQL query.
    """

        actual_prompt = construct_prompt(natural_language_input, top_k, queries, schema)
        logging.debug(f"Actual prompt:\n{actual_prompt}")
        logging.debug(f"Expected prompt:\n{expected_prompt}")
        self.assertEqual(actual_prompt, expected_prompt)
        logging.debug("Finished test_construct_prompt_basic")

if __name__ == '__main__':
    unittest.main()