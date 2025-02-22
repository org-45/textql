import unittest
from src.helper.prompter import construct_prompt


class TestConstructPrompt(unittest.TestCase):

    def test_construct_prompt_basic(self):
        natural_language_input = "Get all users"
        top_k = "Result1\nResult2"
        queries = [
            {"description": "Fetch active users", "sql": "SELECT * FROM users WHERE status = 'active'"}
        ]
        schema = {
            "tables": {
                "users": {
                    "columns": ["id", "name", "email", "status"],
                    "samples": [["1", "John Doe", "john@example.com", "active"]]
                }
            }
        }

        expected_prompt = f"""
    Act as a data analyst and SQL expert. Translate this natural language input into a SQL query for a Postgres DB:
    "{natural_language_input}"

    Schema and sample data:
    Table: users, Columns: id, name, email, status, Sample Data: ['1', 'John Doe', 'john@example.com', 'active']
    
    Reference examples:
    - Fetch active users: SELECT * FROM users WHERE status = 'active'

    Similar results from vector search:
    Result1
Result2

    Ensure the query is safe (no DROP, DELETE, UPDATE) and correct typos based on schema.
    Return only the SQL query.
    """

        actual_prompt = construct_prompt(natural_language_input, top_k, queries, schema)
        self.assertEqual(actual_prompt, expected_prompt)

    def test_construct_prompt_empty_queries(self):
        natural_language_input = "Get all products"
        top_k = "No similar results"
        queries = []
        schema = {
            "tables": {
                "products": {
                    "columns": ["id", "name", "price"],
                    "samples": [["1", "Laptop", "1200"]]
                }
            }
        }

        expected_prompt = f"""
    Act as a data analyst and SQL expert. Translate this natural language input into a SQL query for a Postgres DB:
    "{natural_language_input}"

    Schema and sample data:
    Table: products, Columns: id, name, price, Sample Data: ['1', 'Laptop', '1200']
    
    Reference examples:
    

    Similar results from vector search:
    No similar results

    Ensure the query is safe (no DROP, DELETE, UPDATE) and correct typos based on schema.
    Return only the SQL query.
    """

        actual_prompt = construct_prompt(natural_language_input, top_k, queries, schema)
        self.assertEqual(actual_prompt, expected_prompt)

    def test_construct_prompt_multiple_tables(self):
        natural_language_input = "Get all orders with customer details"
        top_k = "Top result"
        queries = [
            {"description": "Fetch orders by date", "sql": "SELECT * FROM orders WHERE date = '2023-11-15'"}
        ]
        schema = {
            "tables": {
                "orders": {
                    "columns": ["id", "customer_id", "date"],
                    "samples": [["1", "101", "2023-11-16"]]
                },
                "customers": {
                    "columns": ["id", "name", "address"],
                    "samples": [["101", "Alice", "123 Main St"]]
                }
            }
        }

        expected_prompt = f"""
    Act as a data analyst and SQL expert. Translate this natural language input into a SQL query for a Postgres DB:
    "{natural_language_input}"

    Schema and sample data:
    Table: orders, Columns: id, customer_id, date, Sample Data: ['1', '101', '2023-11-16']
Table: customers, Columns: id, name, address, Sample Data: ['101', 'Alice', '123 Main St']
    
    Reference examples:
    - Fetch orders by date: SELECT * FROM orders WHERE date = '2023-11-15'

    Similar results from vector search:
    Top result

    Ensure the query is safe (no DROP, DELETE, UPDATE) and correct typos based on schema.
    Return only the SQL query.
    """

        actual_prompt = construct_prompt(natural_language_input, top_k, queries, schema)
        self.assertEqual(actual_prompt, expected_prompt)


    def test_construct_prompt_no_sample_data(self):
         natural_language_input = "Find all employees"
         top_k = "Similar employee queries"
         queries = []
         schema = {
             "tables": {
                 "employees": {
                     "columns": ["id", "first_name", "last_name"],
                     "samples": []  # No sample data
                 }
             }
         }

         expected_prompt = f"""
    Act as a data analyst and SQL expert. Translate this natural language input into a SQL query for a Postgres DB:
    "{natural_language_input}"

    Schema and sample data:
    Table: employees, Columns: id, first_name, last_name, Sample Data: []
    
    Reference examples:
    

    Similar results from vector search:
    Similar employee queries

    Ensure the query is safe (no DROP, DELETE, UPDATE) and correct typos based on schema.
    Return only the SQL query.
    """

         actual_prompt = construct_prompt(natural_language_input, top_k, queries, schema)
         self.assertEqual(actual_prompt, expected_prompt)

    def test_construct_prompt_multiple_samples(self):
        natural_language_input = "Get all products with prices"
        top_k = "Relevant product results"
        queries = []
        schema = {
            "tables": {
                "products": {
                    "columns": ["id", "name", "price"],
                    "samples": [["1", "Laptop", "1200"], ["2", "Mouse", "25"]]  # Multiple samples
                }
            }
        }

        expected_prompt = f"""
    Act as a data analyst and SQL expert. Translate this natural language input into a SQL query for a Postgres DB:
    "{natural_language_input}"

    Schema and sample data:
    Table: products, Columns: id, name, price, Sample Data: ['1', 'Laptop', '1200']
    
    Reference examples:
    

    Similar results from vector search:
    Relevant product results

    Ensure the query is safe (no DROP, DELETE, UPDATE) and correct typos based on schema.
    Return only the SQL query.
    """

        actual_prompt = construct_prompt(natural_language_input, top_k, queries, schema)
        self.assertEqual(actual_prompt, expected_prompt)


if __name__ == '__main__':
    unittest.main()