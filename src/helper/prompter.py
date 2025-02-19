def construct_prompt(natural_language_input: str, queries: list, schema: dict) -> str:
    """Constructs the prompt for the LLM using the schema, example queries, and user input."""
    reference_prompts = "\n".join([f"- {query['description']}: {query['sql']}" for query in queries])
    table_info = "\n".join([f"Table: {table}, Columns: {', '.join(columns)}" for table, columns in schema["tables"].items()])

    prompt = f"""
    Act as a database analyst and translate the following natural language input into a SQL query:
    "{natural_language_input}"
    Use the following references for guidance. Here are the natural language and its corresponsing sql queries.
    {reference_prompts}
    Here is the schema information. These are the table columns. Understand if a given natural language has a typo as per the given schema info too.
    {table_info}
    Return the SQL query only. No other text.
    """
    return prompt