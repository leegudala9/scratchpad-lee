def get_tagged_coordinates(tagged_co):
    return tuple([float(x) for x in tagged_co.split(" ")])


def get_found_coordinates(found_co):
    return (found_co["x0"], found_co["y0"], found_co["x1"], found_co["y1"])


def overlap(a0, a1, b0, b1):
    return (a0 <= b0 <= a1) or (b0 <= a0 <= b1)


def overlap_coordinates(tagged_co, found_co):
    tagged_co_tup = get_tagged_coordinates(tagged_co)
    found_co_tup = get_found_coordinates(found_co)

    tx0, tx1 = tagged_co_tup[0], tagged_co_tup[2]
    fx0, fx1 = found_co_tup[0], found_co_tup[2]

    ty0, ty1 = tagged_co_tup[1], tagged_co_tup[3]
    fy0, fy1 = found_co_tup[1], found_co_tup[3]
    return overlap(tx0, tx1, fx0, fx1) and overlap(ty0, ty1, fy0, fy1)
