import os
import json
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime

import time
from openai import OpenAI
import openai
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from halo import Halo

from .func_tools import call_my_function, get_table_list
from .utils import download_database_schema

ASSISTANT_NAME="GPTSQL"
client = OpenAI()
GPT_MODEL3="gpt-3.5-turbo-1106"
GPT_MODEL4="gpt-4-1106-preview"
GPT_MODEL=GPT_MODEL4
#GPT_MODEL="gpt-4-1106-preview"

# Replace these with your specific database credentials
db_username = os.environ['DBUSER']
db_password = os.environ['DBPASSWORD']
db_host = os.environ['DBHOST']
db_port = 5432
db_name = os.environ['DBNAME']

class GPTSql:
    FUNCTION_TOOLS = [
        {
            "type": "function",
            "function": {
                "name": "run_sql_command",
                "description": "Execute any SQL command against the Postgres datbase",
                "parameters": {
                    "type": "object", 
                    "properties": {
                        "query": {
                            "type":"string",
                            "description":"Postgres syntax SQL query"
                        }
                    }
                }
            }
        },
    ]
    CONFIG_FILE = os.path.expanduser('~/.gptsql')

    def __init__(self) -> None:
        # PostgreSQL connection string format
        self.connection_string = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

        self.engine = create_engine(self.connection_string)
        # Connect to your database
        self.pgconn = psycopg2.connect(
            f"host={db_host} dbname={db_name} user={db_username} password={db_password}"
        )
        self.config = {
            "assistant_id": None,
            "thread_id": None
        }
        self.thread = None
        self.load_config()
        download_database_schema(self.pgconn)
        self.get_or_create_assistant()

    def save_config(self, key=None, value=None):
        if key and value:
            self.config[key] = value

        for k, v in self.config.items():
            if isinstance(v, datetime):
                self.config[k] = v.isoformat()

        with open(self.CONFIG_FILE, 'w') as f:
            f.write(json.dumps(self.config))

    def load_config(self):
        if os.path.exists(self.CONFIG_FILE):
            with open(self.CONFIG_FILE, 'r') as f:
                self.config = json.loads(f.read())

        for k, v in self.config.items():
            try:
                dt = datetime.fromisoformat(v)
                self.config[k] = dt
            except:
                pass

    def get_or_create_assistant(self):
        # Create or retriveve our Assistant. We also upload the schema file
        # for RAG uses by the assistant.
        self.assistant = None
        if self.config["assistant_id"] is not None:
            try:
                self.assistant = client.beta.assistants.retrieve(self.config["assistant_id"])
            except openai.NotFoundError:
                pass

        if self.assistant is None:
            file = client.files.create(
                file=open("schema.csv", "rb"),
                purpose='assistants'
            )

            print("Creating your PSQL assistant")
            self.assistant = client.beta.assistants.create(
                name=ASSISTANT_NAME,
                instructions="""
You are an assistant helping with data analysis and to query a postgres database. 
You should try to answer questions from knowledge retrieval before relying on a function call.
""",
                tools=[{"type": "code_interpreter"},{"type": "retrieval"}] + self.FUNCTION_TOOLS,
                model=GPT_MODEL,
                file_ids=[file.id]
            )   
            self.save_config("assistant_id", self.assistant.id)

    def chat_loop(self):
        session = PromptSession(history=FileHistory(os.path.expanduser('~/.myhistory')))

        if self.config["thread_id"] is not None:
            thread = client.beta.threads.retrieve(self.config["thread_id"])
        else:
            thread = client.beta.threads.create()
            self.save_config("thread_id", thread.id)

        self.thread = thread

        if self.config["last_run_id"] is not None:
            try:
                client.beta.threads.runs.cancel(thread_id=thread.id, run_id=self.config["last_run_id"])
            except openai.BadRequestError:
                pass
            
        self.last_message_created_at = self.config.get('last_messsage_time')
        breakpoint()
        self.table_list = get_table_list(self.engine)

        spinner = Halo(text='thinking', spinner='dots')
        self.spinner = spinner

        while True:
            try:
                cmd = session.prompt("> ")
                if cmd == "":
                    return
                elif cmd == "history":
                    self.display_messages(show_all=True)
                    continue
                elif cmd == "help":
                    print("""
history - show the complete message history
new thread - start a new thread
                          """)
                    continue
                elif cmd == "new thread":
                    if session.prompt("Do you want to start a new thread (y/n)? ") == "y":
                        thread = client.beta.threads.create()
                        self.save_config("thread_id", thread.id)
                        self.thread = thread
                    continue

                cmd = "This list of tables in the database:\n" + ",".join(self.table_list) + "\n----\n" + cmd
                spinner.start("thinking...")
                self.process_command(thread, cmd)
                spinner.stop()
                self.display_messages()
            except (KeyboardInterrupt, EOFError):
                spinner.stop()
                return

    def display_messages(self, show_all=False):
            messages = client.beta.threads.messages.list(
                thread_id=self.thread.id
            )
            for msg in reversed(list(messages)):
                if msg.role == "user" and not show_all:
                    continue
                if self.last_message_created_at is None or (msg.created_at > self.last_message_created_at) or show_all:
                    self.last_message_created_at = msg.created_at
                    self.save_config("last_messsage_time", self.last_message_created_at)
                    if hasattr(msg.content[0], 'text'):
                        print(f"[{msg.role}] --> {msg.content[0].text.value}")
                    else:
                        print(f"[{msg.role}] --> {type(msg)}")

    def log(self, msg):
        self.spinner.start(msg);
    
    def process_command(self, thread, cmd: str):
        client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=cmd
        )
        runobj = client.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id=self.assistant.id
        )
        self.save_config("last_run_id", runobj.id)
        last_step_count = 0
        while runobj.status not in ["completed", "expired", "cancelled", "failed"]:
            if runobj.status == "in_progress":
                # check for new steps
                run_steps = client.beta.threads.runs.steps.list(
                    thread_id=thread.id,
                    run_id=runobj.id
                )
                run_steps = list(run_steps)
                if len(run_steps) > last_step_count:
                    for step in run_steps[last_step_count:]:
                        for step_detail in step.step_details:
                            #if step_detail[0] in ['tool_calls','message_creation','type']:
                            #    continue
                            if step_detail[0] == 'tool_calls':
                                for tool_call in step_detail[1]:
                                    if 'Function' in str(type(tool_call)):
                                        self.log(f"  --> {tool_call.function.name}()")
                                    elif 'Code' in str(type(tool_call)):
                                        self.log(f"  [code] {tool_call.code_interpreter.input}")
                            #self.spinner.stop()
                            #breakpoint()
                            #pass
                last_step_count = len(run_steps)
            elif runobj.status == "requires_action":
                #print("--> ", runobj.status)
                # Run any functions that the assistant has requested
                if runobj.required_action.type == "submit_tool_outputs":
                    tool_outputs = []
                    for tool_call in runobj.required_action.submit_tool_outputs.tool_calls:
                        res = str(call_my_function(self.engine, tool_call.function.name, json.loads(tool_call.function.arguments)))
                        tool_outputs.append({
                            "tool_call_id": tool_call.id,
                            "output": res
                        })
                    client.beta.threads.runs.submit_tool_outputs(
                        thread_id=thread.id,
                        run_id=runobj.id,
                        tool_outputs=tool_outputs
                    )
                else:
                    print("Unknown action: ", runobj.required_action.type)
            time.sleep(1)
            runobj = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=runobj.id)
            #print(f"status: {runobj.status}, error: {runobj.last_error}")


def main():
    gptsql = GPTSql()
    gptsql.chat_loop()

if __name__ == "__main__":
    main()
