from z3 import *
from collections import ChainMap

class TupleDeclarer:
    declared_tuples = {}
    declared_enums = set()

    def __init__(self, name, fields: dict[str, SortRef], 
                 arr_fields: dict[str, tuple[SortRef, int]]=None, # fixed length
                 vec_fields: dict[str, tuple[SortRef, int]]=None, # variable length
                 short_name=None):
        self.name = name
        self.short_name = short_name or name
        self.fields = fields
        self.vec_fields = vec_fields or {}
        self.flatten_vec_fields: dict[str, SortRef] = {}
        for m in [{f'{fname}__{i}': fsort for i in range(fsize)} | {f'{fname}__length': IntSort()} for fname, (fsort, fsize) in self.vec_fields.items()]:
            self.flatten_vec_fields.update(m)
        self.arr_fields = arr_fields or {}
        self.flatten_arr_fields: dict[str, SortRef] = {}
        for m in [{f'{fname}__{i}': fsort for i in range(fsize)} | {f'{fname}__length': IntSort()} for fname, (fsort, fsize) in self.arr_fields.items()]:
            self.flatten_arr_fields.update(m)
        self.vec_arr_fields = self.vec_fields | self.arr_fields
        for m in [{f'{fname}__{i}': fsort for i in range(fsize)} for fname, (fsort, fsize) in self.arr_fields.items()]:
            self.flatten_arr_fields.update(m)
        self.all_fields = dict(zip(
            list(fields.keys()) + list(self.flatten_vec_fields.keys()) + list(self.flatten_arr_fields.keys()),
            list(fields.values()) + list(self.flatten_vec_fields.values()) + list(self.flatten_arr_fields.values()),
        ))
        self.tuple, self.mk, self.accessors = TupleSort(name, list(fields.values()) + list(self.flatten_vec_fields.values()) + list(self.flatten_arr_fields.values()))
        self.accessors = {k: v for k, v in zip(list(self.fields.keys()) + list(self.flatten_vec_fields.keys()) + list(self.flatten_arr_fields.keys()), self.accessors)}
        self.vec_element_accessor_lists = {fname: [self.accessors[f'{fname}__{i}'] for i in range(fsize)] for fname, (_, fsize) in self.vec_fields.items()}
        self.arr_element_accessor_lists = {fname: [self.accessors[f'{fname}__{i}'] for i in range(fsize)] for fname, (_, fsize) in self.arr_fields.items()}
        self.vec_arr_element_accessor_lists = self.vec_element_accessor_lists | self.arr_element_accessor_lists
        if name in TupleDeclarer.declared_tuples:
            raise ValueError(f"Tuple {name} already defined")
        TupleDeclarer.declared_tuples[name] = self

    def mk_sym(self, prefix):
        prefix = prefix + f'.{self.short_name}'
        values = []
        for fname, fsort in self.all_fields.items():
            if fname.endswith('__length') and fname in self.flatten_arr_fields:
                values.append(self.arr_fields[fname[:-len('__length')]][1])
            elif fsort == IntSort():
                values.append(Int(f'{prefix}_{fname}'))
            elif fsort == BoolSort():
                values.append(Bool(f'{prefix}_{fname}'))
            else:
                if fsort.name() in self.declared_tuples:
                    values.append(self.declared_tuples[fsort.name()].mk_sym(f'{prefix}_{fname}'))
                elif fsort in self.declared_enums:
                    values.append(Const(f'{prefix}_{fname}', fsort))
                else:
                    raise NotImplementedError(f"Unknown sort {fsort}")
            # print(fname, values[-1], fname.endswith('__length'), fname in self.arr_fields)
        return self.mk(*values)
    
    def __getattr__(self, attr):
        if attr in self.accessors:
            return self.accessors[attr]
        elif attr in self.vec_arr_element_accessor_lists:
            return self.vec_arr_element_accessor_lists[attr]
        elif attr.endswith('__select'):
            name = attr[:-len('__select')]
            return lambda s, index, default_value=None: self.select(name, s, index, default_value)
        elif attr.endswith('__store'):
            name = attr[:-len('__store')]
            return lambda s, index, value: self.store(name, s, index, value)
        else:
            raise AttributeError(f"Unknown attribute {attr}")
    
    def update(self, s, kv: dict[str]):
        new_values = []
        assert set(kv.keys()).issubset(set(self.accessors.keys()))
        for filed_name, accessor in self.accessors.items():
            if filed_name in kv:
                new_values.append(kv[filed_name])
            else:
                new_values.append(accessor(s))
        return self.mk(*new_values)
    
    def select(self, name, s, index, default_value=None):
        statement = None
        if default_value is None:
            if self.vec_arr_fields[name][0] == IntSort():
                default_value = 0
            elif self.vec_arr_fields[name][0] == BoolSort():
                default_value = False
            else:
                raise NotImplementedError(f"Must specify default value for {self.vec_arr_fields[name]} sort")
        for i, accessor in enumerate(self.vec_arr_element_accessor_lists[name]):
            if statement is None:
                statement = If(And(index == i, index < self.accessors[f'{name}__length'](s)), accessor(s), default_value)
            else:
                statement = If(And(index == i, index < self.accessors[f'{name}__length'](s)), accessor(s), statement)
        return statement
    
    def store(self, name, s, index, value):
        return self.update(s, {f'{name}__{i}': If(index == i, value, accessor(s)) for i, accessor in enumerate(self.vec_arr_element_accessor_lists[name])})
