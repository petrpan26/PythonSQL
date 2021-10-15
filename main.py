from enum import Enum, auto
from typing import Tuple

# Enums

class MetaCommandResult(Enum):
    META_COMMAND_SUCCESS = auto()
    META_COMMAND_UNRECOGNIZED_COMMAND = auto()

class PrepareResult(Enum):
    PREPARE_SUCCESS = auto()
    PREPARE_UNRECOGNIZED_STATEMENT = auto()
    PREPARE_SYNTAX_ERROR = auto()

class ExecuteResult(Enum):
    EXECUTE_SUCCESS = auto()
    EXECUTE_UNKNOWN = auto()
    EXECUTE_TABLE_FULL = auto()

class StatementType(Enum):
    STATEMENT_BLANK = auto()
    STATEMENT_INSERT = auto()
    STATEMENT_SELECT = auto()

# Constants
ERROR_CODE: dict[int, str] = {
    1: "Something is wrong"
}

# Calculate size of row
ID_SIZE = 4
USERNAME_SIZE = 32
EMAIL_SIZE = 255
ID_OFFSET = 0
USERNAME_OFFSET = ID_OFFSET + ID_SIZE
EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE
ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100
ROWS_PER_PAGE = PAGE_SIZE // ROW_SIZE
TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROWS_PER_PAGE

# class
class Row():
    def __init__(self):
        self.id: int
        self.username: str
        self.email: str

class Statement:
    def __init__(self):
        self.type: StatementType = StatementType.STATEMENT_BLANK
        self.row_to_insert: Row = Row()

class Table:
    def __init__(self) -> None:
        self.num_rows: int = 0
        self.pages: list[bytearray] = [bytearray()] * TABLE_MAX_PAGES
    
    def row_slot(self, row_num: int) -> Tuple[int, int]:
        page_num: int = row_num // ROWS_PER_PAGE

        if len(self.pages[page_num]) == 0:
            self.pages[page_num] = bytearray(b'\x00' * PAGE_SIZE)
        
        row_offset: int = row_num % ROWS_PER_PAGE
        byte_offset: int = row_offset * ROW_SIZE

        return page_num, byte_offset 
    
    @staticmethod
    def to_bytes(input: str, expected_len: int) -> bytes:
        res = str.encode(input)
        len_diff = expected_len - len(res)
        return res + (b'\0' * len_diff)

    def insert_row(self, statement: Statement) -> ExecuteResult:
        if self.num_rows >= TABLE_MAX_ROWS:
            return ExecuteResult.EXECUTE_TABLE_FULL
        
        row: Row = statement.row_to_insert
        
        id, username, email = row.id, row.username, row.email

        page_num, byte_offset = self.row_slot(self.num_rows)

        id_bytes = id.to_bytes(4, byteorder='little')
        username_bytes = self.to_bytes(username, USERNAME_SIZE)
        email_bytes = self.to_bytes(email, EMAIL_SIZE)

        row_content = id_bytes + username_bytes + email_bytes

        for i, c in enumerate(row_content):
            self.pages[page_num][byte_offset + i] = c

        self.num_rows += 1

        print("INSERTED")

        return ExecuteResult.EXECUTE_SUCCESS

    def get_all_rows(self) -> ExecuteResult:
        for row in range(self.num_rows):
            page_num, byte_offset = self.row_slot(row)

            content = self.pages[page_num][byte_offset : byte_offset + ROW_SIZE]

            id = int.from_bytes(content[ID_OFFSET:ID_SIZE], byteorder='little')
            username = content[USERNAME_OFFSET:USERNAME_SIZE].decode().strip('\x00')
            email = content[EMAIL_OFFSET:EMAIL_SIZE].decode().strip('\x00')

            print(id, username, email)

        print("SELECTED")

        return ExecuteResult.EXECUTE_SUCCESS


COLUMN_USERNAME_SIZE = 3
COLUMN_EMAIL_SIZE = 255

# Populate statement information
def prepare_statement(cmd: str, statement: Statement) -> PrepareResult:
    tokens: list[str] = cmd.split()
    if tokens[0] == "insert":
        statement.type = StatementType.STATEMENT_INSERT
        
        try:
            # todo(petrpan26): Hack ??? find better way 
            assert(int(tokens[1]) < 256 ** ID_SIZE)
            assert(len(tokens[2]) <= COLUMN_USERNAME_SIZE)
            assert(len(tokens[3]) <= COLUMN_EMAIL_SIZE)
            statement.row_to_insert.id = int(tokens[1])
            statement.row_to_insert.username = tokens[2]
            statement.row_to_insert.email = tokens[3]
        except:
            return PrepareResult.PREPARE_SYNTAX_ERROR
        
        return PrepareResult.PREPARE_SUCCESS
    elif tokens[0] == "select":
        statement.type = StatementType.STATEMENT_SELECT
        return PrepareResult.PREPARE_SUCCESS
    
    return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT

# Execute sql statement
def execute_statement(statement: Statement, table: Table) -> ExecuteResult:
    if statement.type == StatementType.STATEMENT_SELECT:
        # Do Select
        return table.get_all_rows()
    elif statement.type == StatementType.STATEMENT_INSERT:
        # Do Insert
        return table.insert_row(statement)
    elif statement.type == StatementType.STATEMENT_BLANK:
        exit(1)

    return ExecuteResult.EXECUTE_UNKNOWN

# Handle exit
def sqlExit(code: int) -> None:
    if code != 0:
        print("EXIT CODE {}: {}".format(code, ERROR_CODE[code]))
    else:
        print("Program exit succesfully")
    exit()

# Do meta command 
def do_meta_command(cmd: str) -> MetaCommandResult:
    if cmd == ".exit":
        sqlExit(0)
    return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND

def main() -> None:
    table = Table()
    while True:
        print("sqlhoang>", end=" ")
        cmd: str = input()
        
        if len(cmd) == 0:
            continue
        
        if cmd[0] == ".":
            do_status: MetaCommandResult = do_meta_command(cmd)
            if do_status == MetaCommandResult.META_COMMAND_SUCCESS:
                continue
            elif do_status == MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND:
                print("Unrecognized meta command \"{}\"".format(cmd.split()[0]))
                continue
            else:
                raise KeyError("????")
        
        statement: Statement = Statement()
        
        prepare_status: PrepareResult = prepare_statement(cmd, statement)

        if prepare_status == PrepareResult.PREPARE_SUCCESS:
            pass
        elif prepare_status == PrepareResult.PREPARE_SYNTAX_ERROR:
            print("Syntax error for \"{}\"".format(cmd.split()[0]))
        elif prepare_status == PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT:
            print("Unrecognized keyword  \"{}\"".format(cmd.split()[0]))
        else:
            raise KeyError("????")

        execute_result: ExecuteResult =  execute_statement(statement, table)
        if execute_result == ExecuteResult.EXECUTE_TABLE_FULL:
            print("Table full stop inserting pls !")
        elif execute_result == ExecuteResult.EXECUTE_UNKNOWN:
            print("Something wrong hmm ~~ !")
        print("Executed")

main()
