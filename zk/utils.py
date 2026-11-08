import rich
import re
from tuple_declarer import *


def model_to_dict(model) -> dict:
    text = str(model)
    model_dict = {}
    for line in sorted(text.splitlines()):
        line = line.strip(' ')
        if not line.endswith(','):
            continue
        result = re.search(r"([\w\..]+) = (.+),", line)
        if result is None:
            print(line)
        keys = result.group(1).split('.')
        keys = keys[:1] + [key[key.find('_')+1:] for key in keys[1:]]
        value = result.group(2)
        if keys[0] not in model_dict:
            model_dict[keys[0]] = {}
        cur_dict = model_dict[keys[0]]
        for key in keys[1:-1]:
            if key not in cur_dict:
                cur_dict[key] = {}
            cur_dict = cur_dict[key]
        try:
            cur_dict[keys[-1]] = eval(value)
        except NameError:
            cur_dict[keys[-1]] = value
    return model_dict


def tokens_to_dict(tokens: list[str]) -> dict|int|str:
    assert tokens[0] == '('
    tokens.pop(0)
    tuple_name = tokens.pop(0)
    res = None
    if tuple_name in TupleDeclarer.declared_tuples:
        res = {}
        tuple_declarer = TupleDeclarer.declared_tuples[tuple_name]
        for field_name, accessor in tuple_declarer.accessors.items():
            if tokens[0] == '(':
                res[field_name] = tokens_to_dict(tokens)
            else:
                res[field_name] = tokens.pop(0)
    elif tuple_name == '-':
        assert tokens[1] == ')'
        res = '-' + tokens.pop(0)
    if tokens[0] != ')':
        exit(0)
    tokens.pop(0)
    return res


def sexpr_to_dict(sexpr: str) -> dict:
    TupleDeclarer.declared_tuples
    if '|' in sexpr:
        raise RuntimeError("Cannot handle | in sexpr")
    tokens = []
    for i in range(len(sexpr)):
        match sexpr[i]:
            case '(':
                tokens.append('(')
                tokens.append('')
            case ')':
                tokens.append(')')
                tokens.append('')
            case ' ' | '\n':
                tokens.append('')
            case _:
                tokens[-1] = tokens[-1] + sexpr[i]
    tokens = [token for token in tokens if token != '']
    return tokens_to_dict(tokens)