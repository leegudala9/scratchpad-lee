import csv
from functools import reduce, partial
from inspect import signature


def curry(func):
    arity = lambda func: len(signature(func).parameters)

    def curried(*args):
        aty = arity(func)
        numargs = len(args)
        if aty <= numargs:
            return func(*tuple(args[0:aty]))
        return curry(partial(func, *tuple(args[0:aty])))

    return curried


def identity(x):
    return x


def compose2(f, g):
    return lambda x: f(g(x))


def compose(*functions):
    return reduce(lambda f, g: lambda x: compose2(f, g)(x), functions, identity)


def de_esser(val):
    if type(val) == dict and "S" in val:
        return val["S"]
    return val


@curry
def retain_important_keys(important_keys, element):
    return {k: de_esser(element.get(k)) for k in important_keys}


@curry
def get_attr_list(lst, obj):
    if type(lst) != list or type(obj) not in [list, dict]:
        return obj
    if len(lst) == 0:
        return obj
    head = lst[0]
    tail = lst[1:]
    if type(obj) == dict:
        if head in obj:
            return get_attr_list(tail, obj[head])
        else:
            return None
    elif type(obj) == list:
        if type(head) == int and head < len(obj):
            return get_attr_list(tail, obj[head])
        else:
            return None
    return None


def mapl(lam, lst):
    return list(map(lam, lst))


def rangel(*args):
    return list(range(*args))


def ptorange(element):
    pr = (
        element.get("PageRange", element.get("pagerange"))
        if type(element) == dict
        else None
    )
    if pr is None:
        return []

    if type(pr) == str and "-" in pr:
        tup = pr.split("-")
        return rangel(int(tup[0]), int(tup[1]) + 1)
    return [int(pr)]


def dict_to_tuple(dct):
    return list(dct.items())


def frmat(x):
    return "{0:,.0f}".format(x)


def first(thng):
    if len(thng) < 1:
        return None
    return thng[0]


def time_left(a, b, c):
    full_time = (c / a) * b
    return (full_time - b) / 60


def append_rows(rows, file_name):
    with open(file_name, "a") as f:
        cw = csv.writer(f)
        cw.writerows(rows)


frmlen = compose(frmat, len)

int_first = compose(int, first)


def flatfilter(lst):
    return [x for y in lst for x in y]
