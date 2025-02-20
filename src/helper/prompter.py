from src.vector_comparision import get_similar_rows_from_pg

async def construct_prompt(natural_language_input: str, queries: list, schema: dict) -> str:
    """Constructs the prompt for the LLM using the schema, example queries, and user input."""
    
    reference_prompts = "\n".join([f"- {query['description']}: {query['sql']}" for query in queries])
    table_info = "\n".join([f"Table: {table}, Columns: {', '.join(columns)}" for table, columns in schema['tables'].items()])
    top_k = await get_similar_rows_from_pg(natural_language_input)

    prompt = f"""
    Act as a data analyst and SQL expert. You will translate the following natural language input into a SQL query that will run against a Postgres DB.
    "{natural_language_input}"

    Here is the schema information. These are the tables and columns:
    {table_info}
    
    Do not suggest columns outside these keywords.
    Here are some reference prompts that might come in handy:
    {reference_prompts}

    These are some of the most similar results for the natural language query that were matched from the vector search.
    You can infer data from these results if needed:
    {top_k}

    Also, understand if the given natural language contains any typos as per the provided schema info.

    Return the SQL query only. No other text.
    """
    return prompt