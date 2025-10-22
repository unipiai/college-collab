import streamlit as st
import requests
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_ollama import ChatOllama
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_deepseek import ChatDeepSeek
from langchain_huggingface import HuggingFaceEmbeddings
import os
from dotenv import load_dotenv
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy import text
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain.agents import create_agent
from langchain_community.callbacks import get_openai_callback
from langchain_core.callbacks import BaseCallbackHandler
import time

load_dotenv()

# Custom callback handler to track tokens for non-OpenAI models
class TokenCounterCallback(BaseCallbackHandler):
    def __init__(self):
        self.input_tokens = 0
        self.output_tokens = 0
        self.total_tokens = 0
    
    def on_llm_start(self, serialized, prompts, **kwargs):
        # Approximate token count for input (rough estimation: 1 token ≈ 4 chars)
        for prompt in prompts:
            self.input_tokens += len(prompt) // 4
    
    def on_llm_end(self, response, **kwargs):
        # Track output tokens
        for generation in response.generations:
            for gen in generation:
                self.output_tokens += len(gen.text) // 4
        self.total_tokens = self.input_tokens + self.output_tokens

# Page config
st.set_page_config(
    page_title="SQL Agent with Embeddings",
    page_icon="🤖",
    layout="wide"
)

# Title
st.title("🤖 SQL Database Agent with Semantic Search")
st.markdown("Ask questions about your database in natural language!")

# Sidebar status placeholder to show loading/progress messages
sidebar_status = st.sidebar.empty()

# Sidebar for configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Model selection
    model_choice = st.selectbox(
        "Select LLM Model",
        ["DeepSeek", "OpenAI GPT-4", "Ollama", "Google Gemini"],
        index=0
    )
    
    # Agent mode selection
    agent_mode = st.radio(
        "Agent Mode",
        ["Stream (Real-time)", "Invoke (Stable)"],
        index=1,
        help="Invoke mode is more stable and prevents recursion errors"
    )
    
    st.divider()
    
    # Database info
    st.header("📊 Database Info")
    db_info_ph = st.empty()
    if "db" in st.session_state:
         with db_info_ph.container():
            st.success(f"✅ Connected to database")
            st.info(f"**Dialect:** {st.session_state.db.dialect}")
            if "usable_tables" in st.session_state:
                st.info(f"**Tables:** {len(st.session_state.usable_tables)}")
            else:
                st.info(f"**Tables:** {len(st.session_state.db.get_usable_table_names())}")
    else:
        # Show a sidebar notice while loading; the real spinner is started below
        with sidebar_status.container():
            st.info("Loading Data, please wait...")

    
    st.divider()
    
    # Token stats
    st.header("📈 Token Usage")
    if "token_stats" in st.session_state:
        stats = st.session_state.token_stats
        st.metric("Input Tokens", stats.get("input", 0))
        st.metric("Output Tokens", stats.get("output", 0))
        st.metric("Total Tokens", stats.get("total", 0))
        time_val = stats.get("time_s", None)
        if time_val is not None:
            st.metric("Response Time (s)", f"{time_val:.2f}")
    else:
        st.info("Run a query to see token usage")

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

HISTORY_TURNS = 4

def build_conversation_messages():
    # Clean history for the agent (only role + content)
    return [
        {"role": m["role"], "content": m["content"]}
        for m in st.session_state.messages
        if m.get("role") in ("user", "assistant") and m.get("content")
    ]

def _truncate(text, max_chars=1200):
    try:
        s = str(text)
    except Exception:
        s = repr(text)
    return s if len(s) <= max_chars else s[:max_chars] + " ... [truncated]"

  # Normalize any stream item into a message for stream_mode="messages"
def _as_msg(step):
    # Some runtimes yield (message, metadata), some yield just the message,
    # older ones may yield dicts with "messages"
    try:
        from langchain_core.messages import BaseMessage
    except Exception:
        BaseMessage = object
    if isinstance(step, tuple) and step:
        return step[0]
    if isinstance(step, dict) and "messages" in step:
        return step["messages"][-1]
    # If it's a message already or unknown type, return as-is
    return step
 
  # Extract tool call name and args regardless of provider shape
def _tc_name_args(tc):
    try:
        # Dict-like shapes
        if isinstance(tc, dict):
            # OpenAI tool/function calling shape
            if "function" in tc:
                fn = tc["function"] or {}
                return fn.get("name", ""), fn.get("arguments", {})
            return (
                tc.get("name") or tc.get("type") or "",
                tc.get("args") or tc.get("arguments") or tc.get("input") or {},
            )
        # Object-like shapes
        name = getattr(tc, "name", None) or getattr(tc, "type", "") or ""
        args = (
            getattr(tc, "args", None)
            or getattr(tc, "arguments", None)
            or getattr(tc, "input", None)
            or {}
        )
        return name, args
    except Exception:
        return "", {}

if "db" not in st.session_state:
    # Run initialization with a spinner shown in the sidebar
    with db_info_ph.container():
        with sidebar_status.container():
            with st.spinner("Loading Data, please wait..."):
                try:
                    # Initialize database
                    engine = create_engine(
                        "sqlite:///schools.db",
                        connect_args={"check_same_thread": False, "detect_types": 0},
                        poolclass=StaticPool,
                    )
                    st.session_state.engine = engine
                    st.session_state.db = SQLDatabase(engine=engine)
                    # Exclude helper table from user-facing operations
                    st.session_state.usable_tables = [
                        t for t in st.session_state.db.get_usable_table_names()
                        if t.lower() != "schema_information"
                    ]
                
                    # Initialize model
                    if model_choice == "DeepSeek":
                        st.session_state.model = ChatDeepSeek(model="deepseek-chat")
                    elif model_choice == "OpenAI GPT-4":
                        st.session_state.model = ChatOpenAI(model="gpt-4")
                    elif model_choice == "Ollama":
                        st.session_state.model = ChatOllama(
                            model="qwen2.5-coder:3b",
                            base_url="http://localhost:11434"
                        )
                    else:  # Google Gemini
                        st.session_state.model = ChatGoogleGenerativeAI(model="gemini-2.0-flash-exp")
                    
                    # Initialize embeddings
                    st.session_state.embeddings = HuggingFaceEmbeddings(
                        model_name="all-MiniLM-L6-v2"
                    )
                    
                    def load_schema_info(engine):
                        info = {}
                        try:
                            with engine.connect() as conn:
                                res = conn.execute(
                                    text("SELECT table_name, column_name, description FROM schema_information")
                                )
                                for table_name, column_name, description in res:
                                    info.setdefault(table_name, []).append((column_name, description))
                        except Exception:
                            # If table doesn't exist or is unreadable, silently fallback
                            pass
                        return info
                    
                    # Create schema documents enriched with column descriptions
                    def create_schema_documents(db, usable_tables, schema_info):
                        docs = []
                        for table in usable_tables:
                            try:
                                table_info = db.get_table_info([table])
                                desc_lines = ""
                                if table in schema_info:
                                    parts = [f"- {col}: {desc}" for col, desc in schema_info[table] if desc]
                                    if parts:
                                        desc_lines = "Column descriptions:\n" + "\n".join(parts)

                                content_parts = [f"Table: {table}", table_info]
                                if desc_lines:
                                    content_parts.append(desc_lines)
                                page_content = "\n\n".join(content_parts)

                                doc = Document(
                                    page_content=page_content,
                                    metadata={"table_name": table, "type": "schema"}
                                )
                                docs.append(doc)
                            except Exception as e:
                                st.warning(f"Could not process table {table}: {e}")
                                continue
                        return docs
                    
                    schema_info = load_schema_info(st.session_state.engine)
                    schema_docs = create_schema_documents(
                        st.session_state.db,
                        st.session_state.usable_tables,
                        schema_info
                    )
                    st.session_state.vectorstore = FAISS.from_documents(
                        schema_docs,
                        st.session_state.embeddings
                    )
                    
                    st.success(f"✅ Initialized {len(schema_docs)} table embeddings")
                
                except Exception as e:
                    st.error(f"❌ Error initializing: {str(e)}")
                    st.stop()
                # After the spinner closes, show success status in the sidebar
                st.success("Data Loaded Successfully")
                st.info(f"**Dialect:** {st.session_state.db.dialect}")
                st.info(f"**Tables:** {len(st.session_state.usable_tables) if 'usable_tables' in st.session_state else len(st.session_state.db.get_usable_table_names())}")

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "token_info" in message:
            with st.expander("📊 Token Usage"):
                cols = st.columns(3)
                cols[0].metric("Input", message["token_info"]["input"])
                cols[1].metric("Output", message["token_info"]["output"])
                cols[2].metric("Total", message["token_info"]["total"])

# Chat input
if question := st.chat_input("Ask a question about the database..."):
    # Add user message
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    # Build retrieval query using recent user turns + current question
    past_user_utts = [m["content"] for m in st.session_state.messages if m["role"] == "user"][-(HISTORY_TURNS-1):]
    retrieval_query = " ".join(past_user_utts + [question])
    
    # Get relevant tables using embeddings
    relevant_docs = st.session_state.vectorstore.similarity_search(retrieval_query, k=3)
    relevant_tables = [doc.metadata["table_name"] for doc in relevant_docs]
    
    # Create system prompt
    system_prompt = """
You are an agent designed to interact with a SQL database.
Given an input question, create a syntactically correct {dialect} query to run,
then look at the results of the query and return the answer.

CRITICAL INSTRUCTIONS:
1. First, use sql_db_list_tables to see available tables
2. Then use sql_db_schema to get the schema of relevant tables
3. Write and execute ONE SQL query using sql_db_query
4. Return the answer based on the query results
5. If the query fails, try ONCE more with a corrected query, then stop
6. NEVER loop endlessly - if you can't solve it in 2 query attempts, explain the issue

IMPORTANT NOTES:
- Date columns may be stored as TEXT in DD-MM-YYYY format. Treat them as strings.
- Unless the user specifies a number, limit results to {top_k} rows
- Never query for all columns - only ask for relevant ones
- DO NOT make DML statements (INSERT, UPDATE, DELETE, DROP)
- Order results by a relevant column when appropriate

SEMANTIC HINT: The most relevant tables for this query are likely: {relevant_tables}
Start by examining these tables first.

All available tables: {all_tables}
""".format(
        dialect=st.session_state.db.dialect,
        top_k=5,
        relevant_tables=", ".join(relevant_tables),
        all_tables=st.session_state.usable_tables if "usable_tables" in st.session_state
        else st.session_state.db.get_usable_table_names(),
    )
    
    # Create toolkit and agent
    toolkit = SQLDatabaseToolkit(
        db=st.session_state.db,
        llm=st.session_state.model
    )
    tools = toolkit.get_tools()
    
    agent = create_agent(
        st.session_state.model,
        tools,
        system_prompt=system_prompt,
    )

    # Include full chat history (user + assistant) for context
    conversation_messages = build_conversation_messages()
    
    # Generate response with token tracking
    with st.chat_message("assistant"):
        with st.spinner("🤔 Thinking..."):
            try:
                # Initialize token counter
                token_callback = TokenCounterCallback()
                
                # Configuration with increased recursion limit
                config = {
                    "recursion_limit": 50,  # Increased from default 25
                    "callbacks": [token_callback]
                }

                # Track execution time
                start_time = time.perf_counter()
                
                # Choose between streaming and invoke mode based on sidebar selection
                if agent_mode == "Invoke (Stable)":
                    # Use invoke for more stable execution
                    if model_choice == "OpenAI GPT-4":
                        with get_openai_callback() as cb:
                            result = agent.invoke(
                                {"messages": conversation_messages},
                                config=config
                            )
                            response_text = result["messages"][-1].content
                            input_tokens = cb.prompt_tokens
                            output_tokens = cb.completion_tokens
                            total_tokens = cb.total_tokens
                    else:
                        result = agent.invoke(
                            {"messages": conversation_messages},
                            config=config
                        )
                        response_text = result["messages"][-1].content
                        input_tokens = token_callback.input_tokens
                        output_tokens = token_callback.output_tokens
                        total_tokens = token_callback.total_tokens
                
                else:  # Stream (Real-time) mode
                    # Use streaming mode for real-time updates
                    if model_choice == "OpenAI GPT-4":
                        with get_openai_callback() as cb:
                            response_text = ""
                            step_count = 0
                            max_steps = 1000
                            with st.expander("🧠 Thinking steps", expanded=True):
                                steps_container = st.container()
                                step_counter_ph = st.empty()
                                step_progress = st.progress(0)
                            answer_placeholder = st.empty()
 
                            for step in agent.stream(
                                {"messages": conversation_messages},
                                stream_mode="messages",  # stream individual messages
                                config=config
                            ):
                                step_count += 1
                                if step_count > max_steps:
                                    st.warning(f"⚠️ Stopped after {max_steps} steps to prevent timeout")
                                    break
 
                                # Update step counter and progress bar
                                percent = min(int(step_count / max_steps * 100), 100)
                                step_counter_ph.markdown(f"Step {step_count} of {max_steps}")
                                step_progress.progress(percent)
  
                                msg = _as_msg(step)
 
                                # Tool calls from the AI
                                tool_calls = getattr(msg, "tool_calls", None)
                                if tool_calls:
                                    for tc in tool_calls:
                                        name, args = _tc_name_args(tc)
                                        with steps_container:
                                            st.markdown(f"Step {step_count} • Calling tool: {name}")
                                            if name == "sql_db_query":
                                                sql = args if isinstance(args, str) else (args.get("query") if isinstance(args, dict) else args)
                                                st.code(_truncate(sql), language="sql")
                                            else:
                                                st.code(_truncate(args), language="json")
                                    continue
 
                                # Tool result messages
                                if getattr(msg, "type", "") == "tool":
                                    name = getattr(msg, "name", "")
                                    with steps_container:
                                        st.markdown(f"Step {step_count} ✓ Tool finished: {name}")
                                        st.code(_truncate(getattr(msg, "content", "")), language="text")
                                    continue
 
                                # Final assistant content
                                if getattr(msg, "type", "") == "ai":
                                    content = getattr(msg, "content", "")
                                    if content:
                                        response_text = content
                                        answer_placeholder.markdown(response_text)
                            
                            input_tokens = cb.prompt_tokens
                            output_tokens = cb.completion_tokens
                            total_tokens = cb.total_tokens
                    else:
                        response_text = ""
                        step_count = 0
                        max_steps = 1000
                        with st.expander("🧠 Thinking steps", expanded=True):
                            steps_container = st.container()
                            step_counter_ph = st.empty()
                            step_progress = st.progress(0)
                        answer_placeholder = st.empty()
 
                        for step in agent.stream(
                            {"messages": conversation_messages},
                            stream_mode="messages",
                            config=config
                        ):
                            step_count += 1
                            if step_count > max_steps:
                                st.warning(f"⚠️ Stopped after {max_steps} steps to prevent timeout")
                                break
 
                            # Update step counter and progress bar
                            percent = min(int(step_count / max_steps * 100), 100)
                            step_counter_ph.markdown(f"Step {step_count} of {max_steps}")
                            step_progress.progress(percent)
  
                            msg = _as_msg(step)
 
                            tool_calls = getattr(msg, "tool_calls", None)
                            if tool_calls:
                                for tc in tool_calls:
                                    name, args = _tc_name_args(tc)
                                    with steps_container:
                                        st.markdown(f"Step {step_count} • Calling tool: {name}")
                                        if name == "sql_db_query":
                                            sql = args if isinstance(args, str) else (args.get("query") if isinstance(args, dict) else args)
                                            st.code(_truncate(sql), language="sql")
                                        else:
                                            st.code(_truncate(args), language="json")
                                continue
 
                            if getattr(msg, "type", "") == "tool":
                                name = getattr(msg, "name", "")
                                with steps_container:
                                    st.markdown(f"Step {step_count} ✓ Tool finished: {name}")
                                    st.code(_truncate(getattr(msg, "content", "")), language="text")
                                continue
 
                            if getattr(msg, "type", "") == "ai":
                                content = getattr(msg, "content", "")
                                if content:
                                    response_text = content
                                    answer_placeholder.markdown(response_text)
                        
                        input_tokens = token_callback.input_tokens
                        output_tokens = token_callback.output_tokens
                        total_tokens = token_callback.total_tokens
                        
                elapsed_s = time.perf_counter() - start_time
                
                # Display response
                if response_text:
                    st.markdown(response_text)
                else:
                    st.warning("⚠️ No response generated. The query may have exceeded the step limit.")
                    response_text = "Query processing stopped - may need simplification or database contains complex data."
                
                # Display token info
                token_info = {
                    "input": input_tokens,
                    "output": output_tokens,
                    "total": total_tokens,
                    "time_s": round(elapsed_s, 2),
                }
                
                with st.expander("📊 Token Usage"):
                    cols = st.columns(3)
                    cols[0].metric("Input Tokens", input_tokens)
                    cols[1].metric("Output Tokens", output_tokens)
                    cols[2].metric("Total Tokens", total_tokens)

                 # NEW: Show timing in an expander
                with st.expander("⏱️ Timing"):
                    st.metric("Response Time (s)", f"{elapsed_s:.2f}")
                
                # Show relevant tables
                with st.expander("🔍 Relevant Tables (via Embeddings)"):
                    st.write(", ".join(relevant_tables))
                
                # Add to messages
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": response_text,
                    "token_info": token_info
                })
                
                # Update session state token stats
                st.session_state.token_stats = token_info
                
            except Exception as e:
                error_msg = f"❌ Error: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })

# Footer
st.divider()
st.caption("💡 Tip: Ask questions like 'Show me colleges by state' or 'Which subjects are offered?'")