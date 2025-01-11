from api.main import get_datas


collection = ""
column_schema = "Nom:value,Valeur:avg".split(",")
token = ""

res = get_datas(collection, column_schema, token)


get_value = {
    "title": lambda x: x["title"][0]['plain_text'],
    "number": lambda x: x["number"]
}


def get_value_from_prop(properties, prop):
    _type = properties[prop]["type"]
    return get_value[_type](properties[prop])


column_names = list(map(lambda x: x.split(":")[0], column_schema))
data = [
    [get_value_from_prop(page["properties"], prop) for prop in column_names] for page in res
]

from itertools import groupby

groups = groupby(data, key=lambda x: x[0])


series = []

for key, group in groups:
    serie = [key]
    for i, schema in enumerate(column_schema[1:]):
        col, action = schema.split(":")

        if action == 'count':
            serie.append(len(group))
        elif action == 'sum':
            serie.append(sum(map(lambda x: x[i+1], group)))
        elif action == 'avg':
            group = list(group)
            serie.append(sum(map(lambda x: x[i+1], group)) / len(list(group)))
        elif action == 'value':
            serie.append(",".join(list(map(lambda x: str(x[i + 1]), group))))
        else:
            raise RuntimeError(f"action {action} not implemented")
    series.append(serie)


for k,g in groups:
    print(k, list(g))