##TextQL

Natural language to SQL query translation pipeline with frontend.

**Features**

*   **Natural Language Processing**: Converts user queries (e.g., "Show flights from JFK") into SQL using Google’s Gemini API.
    
*   **Vector Search**: Enhances query generation with similar past queries via Sentence Transformers and pgvector.
    
*   **Asynchronous Database Access**: Uses asyncpg for efficient PostgreSQL interactions with a 10-second query timeout.
    
*   **Interactive UI**: HTMX-driven frontend with Jinja2 templates for real-time query execution and feedback.
    
*   **Rate Limiting**: Limits API requests (e.g., 1 per 15 seconds) using slowapi.
    
*   **Feedback Loop**: Allows users to submit corrections, stored in the database for future improvements.
    
*   **Configurable**: Non-sensitive settings in textql.yaml, secrets in .env.


Data flows from natural language input to SQL generation, execution, and feedback storage, leveraging embeddings for context.


**Architecture**
```mermaid
sequenceDiagram
    participant U as User
    participant A as FastAPI + HTMX UI
    participant I as Importer
    participant DB as Database Manager (PostgreSQL)
    participant R1 as Routes: /generate-sql
    participant L as Loader
    participant V as Vector Search
    participant P as Prompter
    participant G as LLM Generator
    participant F as Query Formatter
    participant Val as Query Validator
    participant T as Query Token Store
    participant R2 as Routes: /execute-sql
    participant R3 as Routes: /submit-feedback
    participant K as Gemini API

    %% Step 1: Initialization via Importer
    note right of I: Runs first to initialize DB
    I->>DB: Initialize database (create tables, import CSV)
    DB-->>I: Tables created, data imported
    I->>DB: Insert embeddings into Text Embeddings
    DB-->>I: Embeddings stored

    %% Step 2: App Startup
    note right of A: App starts after initialization
    A->>DB: Initialize connection pool

    %% Step 3: User Interaction - Generate SQL
    U->>A: Submit NL Query (e.g., "Show flights from JFK")
    A->>R1: POST /generate-sql
    R1->>G: Sanitized query
    G->>L: Fetch schema and samples
    L->>DB: Get schema (flights, airlines, airports)
    DB-->>L: Schema data
    L->>DB: Get sample data
    DB-->>L: Sample rows
    L->>V: Trigger vector search
    V->>DB: Query Text Embeddings for similar rows
    DB-->>V: Similar rows
    V-->>L: Return similar rows
    L->>P: Schema, samples, similar rows
    P-->>G: Formatted prompt
    G->>K: API call with prompt
    K-->>G: Raw SQL
    G->>F: Format raw SQL
    F-->>G: Formatted SQL
    G->>Val: Validate SQL
    Val-->>G: Validated SQL
    G->>T: Store SQL with token
    T-->>R1: Token + SQL
    R1-->>A: Render HTML with SQL and token
    A-->>U: Display generated SQL

    %% Step 4: Execute SQL
    U->>A: Click Execute (POST /execute-sql)
    A->>R2: POST /execute-sql with token
    R2->>T: Retrieve SQL by token
    T-->>R2: SQL
    R2->>DB: Execute SQL
    DB-->>R2: Results/Error
    R2-->>A: Render HTML with results
    A-->>U: Display results

    %% Step 5: Submit Feedback
    U->>A: Submit feedback (Yes/No, optional correction)
    A->>R3: POST /submit-feedback with token
    R3->>T: Retrieve original SQL
    T-->>R3: SQL
    R3->>DB: Store feedback (NL, SQL, correction)
    DB-->>R3: Feedback stored
    R3-->>A: Render feedback response
    A-->>U: Display feedback confirmation
```

**Prerequisites**

*   **Python**: 3.9+
    
*   **PostgreSQL**: 13+ with pgvector extension
    
*   **Dependencies**: Listed in requirements.txt
    
*   **Google Gemini API Key**: For LLM integration
    

**Installation**

1.  clone https://github.com/chauchausoup/textql.git

2.  copy .env file
    
3.  docker-compose up --build -d

4.  ./init.sh


**Usage**

1.  **Visit the Root Page**: Open http://127.0.0.1:8000 to see the TextQL interface.
    
2.  **Generate SQL**:
    
    *   Enter a natural language query (e.g., "Show flights from JFK").
        
    *   Submit via the form (POST /generate-sql).
        
    *   View the generated SQL query.
        
3.  **Execute SQL**:
    
    *   Click "Execute Query" (POST /execute-sql using the query\_token).
        
    *   See results in a table.
        
4.  **Submit Feedback**:
    
    *   Approve ("Yes") or correct ("No") the query (POST /submit-feedback).
        
    *   Provide a corrected SQL query if needed.
        

**API Endpoints**While primarily a web app, the endpoints are accessible:

*   **GET /**: Root page (HTML).
    
*   **POST /generate-sql**: Generates SQL from natural language input.
    
    *   **Body**: natural\_language\_input (form-data).
        
    *   **Response**: HTML with SQL and token.
        
*   **POST /execute-sql**: Executes a generated SQL query.
    
    *   **Body**: query\_token (form-data).
        
    *   **Response**: HTML with results or error.
        
*   **POST /submit-feedback**: Submits feedback on a query.
    
    *   **Body**: query\_token, feedback, corrected\_sql (optional, form-data).
        
    *   **Response**: HTML with feedback result.
        

**Rate Limits**:

*   /generate-sql, /execute-sql: 1 request per 15 seconds.
    
*   /submit-feedback: 1 request per 5 seconds.


