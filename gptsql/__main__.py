import argparse
from datetime import datetime
import importlib.metadata
import json
import os
import psycopg2
import singlestoredb as s2
import time
import toml

from openai import OpenAI
import openai
from prompt_toolkit import PromptSession, prompt
from prompt_toolkit.history import FileHistory
from halo import Halo
from sqlalchemy import create_engine

from .func_tools import call_my_function, get_table_list

ASSISTANT_NAME="GPTSQL"
GPT_MODEL3="gpt-3.5-turbo-1106"
GPT_MODEL4="gpt-4-1106-preview"
GPT_MODEL=GPT_MODEL4
#GPT_MODEL="gpt-4-1106-preview"

POSTGRES_DEFAULT_PORT = 5432
SINGLESTORE_DEFAULT_PORT = 3306

# Replace these with your specific database credentials

class GPTSql:
    FUNCTION_TOOLS = [
        {
            "type": "function",
            "function": {
                "name": "run_sql_command",
                "description": "Execute any SQL command against the SingleStore/Postgres datbase",
                "parameters": {
                    "type": "object", 
                    "properties": {
                        "query": {
                            "type":"string",
                            "description":"SingleStore/Postgres syntax SQL query"
                        }
                    }
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "show_long_query_results_on_demand",
                "description": "Only call this function if the user requests to 'print all results'",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                }
            }
        },
    ]
    CONFIG_FILE = os.path.expanduser('~/.gptsql')

    def __init__(self) -> None:
        self.load_config()

        args = self.parse_args()

        if 'DBUSER' in self.config and 'DBHOST' in self.config:
            db_type = self.config['DBTYPE']
            db_username = self.config['DBUSER']
            db_password = self.config['DBPASSWORD']
            db_host = self.config['DBHOST']
            db_port = int(self.config['DBPORT'])
            db_name = self.config['DBNAME']
        else:
            db_type = args.dbtype or os.environ.get('DBTYPE')
            db_username = args.username or os.environ.get('DBUSER')
            db_password = args.password or os.environ.get('DBPASSWORD')
            db_host = args.host or os.environ.get('DBHOST')
            if args.port:
                db_port = args.port
            else:
                db_port = POSTGRES_DEFAULT_PORT if db_type == 'PostgreSQL' else SINGLESTORE_DEFAULT_PORT
            db_name = args.dbname or os.environ.get('DBNAME')

        if db_host is None:
            connection_good = False
            while not connection_good:
                print("Let's setup your database connection...")
                choice = prompt("Choose the database type (SingleStore (1) or PostgreSQL (2)): ")
                db_type = 'PostgreSQL' if choice == '2' else 'SingleStore'
                db_host = prompt("Enter your database host: ")
                db_username = prompt("Enter your database username: ")
                db_password = prompt("Enter your database password: ", is_password=True)
                db_name = prompt("Enter the database name: ")
                default_port = POSTGRES_DEFAULT_PORT if db_type == 'PostgreSQL' else SINGLESTORE_DEFAULT_PORT
                db_port = prompt("Enter your database port ({}): ".format(default_port)) or default_port
                db_port = int(db_port)
                print("Validating connection info...")
                try:
                    if db_type == 'PostgreSQL':
                        conn = psycopg2.connect(
                            f"host={db_host} dbname={db_name} user={db_username} password={db_password}",
                            connect_timeout=10
                        )
                    else:
                        conn = s2.connect(host=db_host, user=db_username, password=db_password, port=db_port)
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT version();")
                    connection_good = True
                except (s2.OperationalError, psycopg2.OperationalError) as e:
                    print("Error: ", e)
                    continue
                self.config |= {
                    "DBUSER": db_username,
                    "DBPASSWORD": db_password,
                    "DBHOST": db_host,
                    "DBPORT": db_port,
                    "DBNAME": db_name,
                    "DBTYPE": db_type
                }

            self.save_config()

        # connection string format
        self.db_config = {
            'db_username': db_username,
            'db_password': db_password,
            'db_host': db_host,
            'db_port': db_port,
            'db_name': db_name,
            'db_type': db_type
        }
        if db_type == 'PostgreSQL':
            self.connection_string = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
        else:
            self.connection_string = f'mysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

        self.engine = create_engine(self.connection_string)
        # Connect to your database
        if db_type == 'PostgreSQL':
            self.conn = psycopg2.connect(
                f"host={db_host} dbname={db_name} user={db_username} password={db_password}"
            )
        else:
            self.conn = s2.connect(host=db_host, user=db_username, password=db_password, port=db_port, database=db_name)
        self.thread = None

        api_key = self.config.get('OPENAI_API_KEY') or os.environ.get('OPENAI_API_KEY')
        if api_key is None:
            api_key = prompt("Enter your Open AI API key: ", is_password=True)
            self.save_config("OPENAI_API_KEY", api_key)

        if 'model' not in self.config:
            print("Which model do you want to use?")
            print(f"1. {GPT_MODEL3}")
            print(f"2. {GPT_MODEL4}")
            choice = prompt("(1 or 2) >")
            if choice == "1":
                self.save_config("model", GPT_MODEL3)
            else:
                self.save_config("model", GPT_MODEL4)

        self.oaclient = OpenAI(api_key=api_key)
        self.get_or_create_assistant()

    def parse_args(self):
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument('-help', '--help', action='help', default=argparse.SUPPRESS, help='Show this help message and exit')

        parser.add_argument('-dt', '--dbtype', type=str, required=False)
        parser.add_argument('-h', '--host', type=str, required=False)
        parser.add_argument('-p', '--port', type=int, required=False)
        parser.add_argument('-U', '--username', type=str, required=False)
        parser.add_argument('-d', '--dbname', type=str, required=False)
        parser.add_argument('--password', type=str, required=False)

        return parser.parse_args()
    
    def save_config(self, key=None, value=None):
        if key and value:
            self.config[key] = value

        for k, v in self.config.items():
            if isinstance(v, datetime):
                self.config[k] = v.isoformat()

        with open(self.CONFIG_FILE, 'w') as f:
            f.write(json.dumps(self.config))

    def load_config(self):
        self.config = {}
        if os.path.exists(self.CONFIG_FILE):
            with open(self.CONFIG_FILE, 'r') as f:
                self.config = json.loads(f.read())

        for k, v in self.config.items():
            try:
                # TODO: is this necessary?
                # dt = datetime.fromisoformat(v)
                self.config[k] = v
            except Exception as e:
                print("Error1: ", e)
                pass

    def get_version(self):
        try:
            pyproject = toml.load(os.path.join(os.path.dirname(__file__), "..", "pyproject.toml"))
            return pyproject["tool"]["poetry"]["version"]
        except Exception as e:
            print("Error2: ", e)
            return importlib.metadata.version("gptsql")

    def get_or_create_assistant(self):
        # Create or retriveve our Assistant. We also upload the schema file
        # for RAG uses by the assistant.
        self.assistant = None
        if self.config.get("assistant_id") is not None:
            try:
                self.assistant = self.oaclient.beta.assistants.retrieve(self.config["assistant_id"])
            except openai.NotFoundError as e:
                print("Assistant not found: ", e)
                pass

        if self.assistant is None:
            print("Creating your PSQL assistant")
            self.assistant = self.oaclient.beta.assistants.create(
                name=ASSISTANT_NAME,
                instructions="""
You are an assistant helping with data analysis and to query a postgres/singlestoredb database. 
""",
                tools=[{"type": "code_interpreter"}] + self.FUNCTION_TOOLS,
                # tools=[{"type": "code_interpreter"}, {"type": "retrieval"}] + self.FUNCTION_TOOLS,
                model=self.config['model']
            )   
            self.save_config("assistant_id", self.assistant.id)

    def chat_loop(self):
        session = PromptSession(history=FileHistory(os.path.expanduser('~/.myhistory')))

        if self.config.get("thread_id") is not None:
            thread = self.oaclient.beta.threads.retrieve(self.config["thread_id"])
        else:
            thread = self.oaclient.beta.threads.create()
            self.save_config("thread_id", thread.id)

        self.thread = thread

        if self.config.get("last_run_id") is not None:
            try:
                self.oaclient.beta.threads.runs.cancel(thread_id=thread.id, run_id=self.config["last_run_id"])
            except(openai.BadRequestError, openai.NotFoundError) as e:
                print("Error4: ", e)
                pass
            
        self.last_message_created_at = self.config.get('last_messsage_time')
        self.table_list = get_table_list(self.engine, self.db_config.get("db_name"))

        spinner = Halo(text='thinking', spinner='dots')
        self.spinner = spinner

        print("""
Welcome to GPTSQL, the chat interface to your SingleStore/Postgres database.
You can ask questions like:
    "help" (show some system commands)
    "show all the tables"
    "show me the first 10 rows of the users table"
    "show me the schema for the orders table"
        """)
        while True:
            try:
                cmd = session.prompt("\n> ")
                if cmd == "":
                    continue
                elif cmd == "history":
                    self.display_messages(show_all=True)
                    continue
                elif cmd == "help":
                    print("""
connection - show the database connection info
history - show the complete message history
new thread - start a new thread
exit
                          """)
                    continue
                elif cmd == "new thread":
                    if session.prompt("Do you want to start a new thread (y/n)? ") == "y":
                        thread = self.oaclient.beta.threads.create()
                        self.save_config("thread_id", thread.id)
                        self.thread = thread
                    continue
                elif cmd == "connection":
                    print(f"Host: {self.db_config['db_host']}, Database: {self.db_config['db_name']}, User: {self.db_config['db_username']}")
                    print(f"Model: {self.assistant.model}")
                    print(f"Version: {self.get_version()}")
                    continue
                elif cmd == "exit":
                    return

                cmd = "These are the tables in the database:\n" + ",".join(self.table_list) + "\n----\n" + cmd
                print(cmd)
                spinner.start("thinking...")
                self.process_command(thread, cmd)
                spinner.stop()
                self.display_messages()
            except (KeyboardInterrupt, EOFError) as e:
                print("Error5: ", e)
                spinner.stop()
                return

    def display_messages(self, show_all=False):
            messages = self.oaclient.beta.threads.messages.list(
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
        #print(msg)
    
    def process_command(self, thread, cmd: str):
        self.oaclient.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=cmd
        )
        runobj = self.oaclient.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id=self.assistant.id
        )
        self.save_config("last_run_id", runobj.id)
        last_step_count = 0
        while runobj.status not in ["completed", "expired", "cancelled", "failed"]:
            if runobj.status == "in_progress":
                # check for new steps
                run_steps = self.oaclient.beta.threads.runs.steps.list(
                    thread_id=thread.id,
                    run_id=runobj.id
                )
                run_steps = list(run_steps)
                #print(run_steps)
                #print("\n\n")
                if len(run_steps) > last_step_count:
                    for step in run_steps[last_step_count:]:
                        for step_detail in step.step_details:
                            if step_detail[0] == 'tool_calls':
                                for tool_call in step_detail[1]:
                                    #if 'Function' in str(type(tool_call)):
                                    #    self.log(f"  --> {tool_call.function.name}()")
                                    if 'Code' in str(type(tool_call)):
                                        self.log(f"  [code] {tool_call.code_interpreter.input}")
                last_step_count = len(run_steps)
            elif runobj.status == "requires_action":
                # Run any functions that the assistant has requested
                if runobj.required_action.type == "submit_tool_outputs":
                    tool_outputs = []
                    for tool_call in runobj.required_action.submit_tool_outputs.tool_calls:
                        self.log(f"  --> {tool_call.function.name}()")
                        res = str(call_my_function(self.engine, tool_call.function.name, json.loads(tool_call.function.arguments)))
                        tool_outputs.append({
                            "tool_call_id": tool_call.id,
                            "output": res
                        })
                    self.oaclient.beta.threads.runs.submit_tool_outputs(
                        thread_id=thread.id,
                        run_id=runobj.id,
                        tool_outputs=tool_outputs
                    )
                    self.spinner.text = "considering results..."
                else:
                    print("Unknown action: ", runobj.required_action.type)
            time.sleep(1)
            runobj = self.oaclient.beta.threads.runs.retrieve(thread_id=thread.id, run_id=runobj.id)


def main():
    gptsql = GPTSql()
    gptsql.chat_loop()

if __name__ == "__main__":
    main()

