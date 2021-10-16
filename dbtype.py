
class Type():
    def __init__(self, is_pk: bool = False):
        self.is_pk = is_pk

    def encode(self, input: str) -> str:
        return '0'

INT_MAX_LEN = 10
STR_MAX_LEN = 100
MAX_NUM = int('9' * INT_MAX_LEN)

class Int(Type):
    def encode(self, input: str) -> str:
        # TODO(petrpan26): Change this pls
        signed = '0' if int(input) < 0 else '1'
        value = int(input)
        value += MAX_NUM + 1 if value < 0 else 0 
        value_str = str(value)
        res = signed + '0' * (INT_MAX_LEN - len(value_str)) + value_str

        return res

class String(Type):
    def encode(self, input: str) -> str:
        return input
    

