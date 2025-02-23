def construct_prompt(natural_language_input: str, top_k: str, queries: list, schema: dict) -> str:
    """Constructs an enhanced prompt for the LLM."""
    reference_prompts = "\n".join([f"- {query['description']}: {query['sql']}" for query in queries])
    table_info = "\n".join([f"""Table: {table},Columns: {', '.join(data['columns'])}"""for table, data in schema['tables'].items()])

    prompt = f"""
    Act as a data analyst and SQL expert. Translate this natural language input into a SQL query for a Postgres DB:
    "{natural_language_input}"

    Schema:
    {table_info}
    
    Reference examples:
    {reference_prompts}

    Similar results from vector search:
    {top_k}

    Ensure the query is safe (no DROP, DELETE, UPDATE) and correct typos based on schema.
    Return only the SQL query.
    """
    return prompt