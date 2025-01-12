from typing import Any

from flask import Flask, render_template, redirect, request, jsonify
from os.path import abspath, dirname, join, realpath
from werkzeug.exceptions import HTTPException
from flask.wrappers import Response
import traceback
import json
import requests
from itertools import groupby



dir_path = dirname(realpath(__file__))
app = Flask(__name__, template_folder=join(dir_path, abspath('templates')))

g = {}
IMG_WIDTH = 380
IMG_HEIGHT = 220

TYPES_EXCLUDED = ['relation', 'person', 'date']
CHART_URL = f'https://quickchart.io/chart?w={IMG_WIDTH}&h={IMG_HEIGHT}'
NOTION_API_BASE_URL = "https://api.notion.com/"
NOTION_VERSION = '2022-06-28'

NOTION_PROPERTY_VALUE = {
    "title": lambda x: x["title"][0]['plain_text'],
    "number": lambda x: x["number"],
    "date": lambda x: x["date"]["start"],
    "array": lambda x: NOTION_PROPERTY_VALUE[x["array"][0]["type"]](x["array"][0]),
    "formula": lambda x: NOTION_PROPERTY_VALUE[x["formula"]["type"]](x["formula"]),
    "rollup": lambda x:  NOTION_PROPERTY_VALUE[x["rollup"]["type"]](x["rollup"]),
    "rich_text": lambda x: x["rich_text"][0]["plain_text"]
}


def get_value_from_prop(properties: dict, prop: str, mapper: dict, relation_lookup, token) -> Any:
    """
    Get the value of a Notion property.

    :param properties: dictionary of page properties
    :param prop: the name of the property
    :param mapper: key : type of the property. Value a callable giving the value.
    """
    _type = properties[prop]["type"]
    if _type == "relation":
        related_id = properties[prop][_type][0]['id']
        if related_id in relation_lookup:
            return relation_lookup[related_id]
        else:
            res = requests.get(
                f"{NOTION_API_BASE_URL}v1/pages/{related_id}/properties/title",
                headers={"Authorization": f"Bearer {token}", "Notion-Version": NOTION_VERSION},
            ).json()
            value = res["results"][0]["title"]["plain_text"]
            relation_lookup[related_id] = value
            return value
    else:
        return mapper[_type](properties[prop])


def aggregate(datas, column_schema):
    column_names = list(map(lambda x: x.split(":")[0], column_schema))

    datas.sort(key=lambda x: x[0])

    groups = groupby(datas, key=lambda x: x[0])

    series = []

    for key, group in groups:
        serie = [key]
        group = list(group)
        for i, schema in enumerate(column_schema[1:]):
            col, action = schema.split(":")

            if action == 'count':
                serie.append(len(list(filter(lambda x: x[i + 1] is not None, group))))
            elif action == 'sum':
                serie.append(sum(map(lambda x: x[i + 1] if x[i + 1] is not None else 0, group)))
            elif action == 'avg':
                group = list(group)
                _serie = [x[i + 1] for x in group if x[i + 1] is not None]
                serie.append(sum(_serie) / len(_serie) if len(_serie) > 0 else 0)
            elif action == 'value':
                serie.append(",".join(list(map(lambda x: str(x[i + 1]) if x[i + 1] is not None else "", list(group)))))
            else:
                raise RuntimeError(f"action {action} not implemented")
        series.append(serie)

    return [
        column_names
    ] + series


def aggregate_split(datas:list, column_schema):
    serie_labels = sorted(list(set([x[-1] for x in datas])))
    datas.sort(key=lambda x: x[0])
    group_by_label = groupby(datas, lambda x: x[0])

    series = []
    _, action = column_schema[1].split(":")

    for label, group_label in group_by_label:
        serie = [label]
        group_label = sorted(list(group_label), key=lambda x: x[-1])
        for serie_label in serie_labels:
            value = 0
            group_by_serie = groupby(group_label, lambda x: x[-1])
            for serie_name, group in group_by_serie:
                if serie_label == serie_name:
                    group = list(group)
                    if action == 'count':
                        value = len(list(filter(lambda x: x[1] is not None, group)))
                    elif action == 'sum':
                        value = sum(map(lambda x: x[1] if x[1] is not None else 0, group))
                    elif action == 'avg':
                        group = list(group)
                        _serie = [x[1] for x in group if x[1] is not None]
                        value = sum(_serie) / len(_serie) if len(_serie) > 0 else 0
                    elif action == 'value':
                        value = ",".join(list(map(lambda x: str(x[1]) if x[1] is not None else "", list(group))))
                    else:
                        raise RuntimeError(f"action {action} not implemented")
                    break
            serie.append(value)
        series.append(serie)

    return [
        [column_schema[0].split(":")[0]] + serie_labels
    ] + series





def remove_non_ascii(string):
    return bytes(string, 'utf-8').decode('ascii', 'ignore')


def flatten_row(row):
    res = []

    for field, value in row.items():
        if value and (isinstance(value, list) or (isinstance(value, str) and ',' in value)):
            if not isinstance(value, list):
                value = value.split(',')
            for v in value:
                res += flatten_row({**row, field: v or 'EMPTY'})
        elif value == [] or value == None:
            return flatten_row({**row, field: 'EMPTY'})

    return res or [row]


def clean_data(rows, fields):
    res = []
    rows = [{field: row.get_property(field)
             for field in fields} for row in rows]
    for row in rows:
        res += flatten_row(row)
    return res


def get_datas(collection: str, column_schema: list, notion_bearer_token: str, db_filter=None, split=None):
    headers = {"Authorization": f"Bearer {notion_bearer_token}", "Notion-Version": NOTION_VERSION}
    column_names = list(map(lambda x: x.split(":")[0], column_schema)) + [split]

    # Get propery IDs
    res = requests.get(
        f"{NOTION_API_BASE_URL}v1/databases/{collection}",
        headers=headers,
    ).json()

    print(res)

    property_ids = [res["properties"].get(name).get("id") for name in column_names]

    filter_properties = "&".join([f"filter_properties={name}" for name in property_ids])
    has_more = True
    pages = []
    next_cursor = None
    while has_more:
        _json = {}
        if next_cursor is not None:
            _json["start_cursor"] = next_cursor
        if db_filter is not None:
            _json["filter"] = db_filter
        current = requests.post(
            f"{NOTION_API_BASE_URL}v1/databases/{collection}/query?{filter_properties}",
            headers={"Authorization": f"Bearer {notion_bearer_token}", "Notion-Version": NOTION_VERSION},
            json=_json,
        ).json()

        pages += current["results"]
        has_more = current.get("has_more", False)
        next_cursor = current.get("next_cursor")

    relation_lookup = {}

    data = [
        [get_value_from_prop(page["properties"], prop, NOTION_PROPERTY_VALUE, relation_lookup, notion_bearer_token) for prop in column_names] for page in
        pages
    ]

    class Foo:
        name = column_names[0]

    aggregated = aggregate(data, column_schema) if not split else aggregate_split(data, column_schema)

    return Foo(), aggregated


@app.errorhandler(Exception)
def handle_error(e):
    """
    Handle and display errors to client.
    Log error traceback.
    """
    code = 500
    if isinstance(e, HTTPException):
        code = e.code
    print(traceback.format_exc())
    return jsonify(error=str(e)), code


@app.route('/schema-chart/<collection>')
def build_schema_chart(collection):
    """
    Schema chart is the interactive chart.
    """
    dark_mode = 'dark' in request.args
    chart_type = request.args.get('t', 'PieChart')
    columns_schema = request.args.get('s', '').split(',')
    token = request.args.get("token")
    _filter = json.loads(request.args.get("f")) if request.args.get("f") is not None else None
    split = request.args.get("split")

    cv, datas = get_datas(collection, columns_schema, token, db_filter=_filter ,split=split)

    if request.headers.get('sec-ch-prefers-color-scheme') == 'dark':
        dark_mode = True

    resp = Response(render_template(
        'schema.html',
        dark_mode=dark_mode,
        chart_type=chart_type,
        datas=json.dumps(datas),
        title=request.args.get('title', cv.name),
    ))
    resp.headers['Vary'] = 'Sec-CH-Prefers-Color-Scheme'
    resp.headers['Accept-CH'] = 'Sec-CH-Prefers-Color-Scheme'
    resp.headers['Critical-CH'] = 'Sec-CH-Prefers-Color-Scheme'
    return resp


@app.route('/image-chart/<collection>')
def build_image_chart(collection):
    """
    Return an image chart generated by quickchart.io.
    """
    dark_mode = 'dark' in request.args
    chart_type = request.args.get('t', 'PieChart')
    columns_schema = request.args.get('s', '').split(',')
    token = request.args.get("token")
    _filter = json.loads(request.args.get("f")) if request.args.get("f") is not None else None
    split = request.args.get("split")

    _, datas = get_datas(collection, columns_schema, token, db_filter=_filter ,split=split)

    force_white_labels = {'legend': {'labels': {'fontColor': 'white'}}}
    labels = list(map(lambda x: remove_non_ascii(x[0]), datas[1:])) # x axis
    datasets = []

    nb_datasets = len(datas[0])

    for index in range(1, nb_datasets):
        datasets.append({
            'label': datas[0][index], # name of the serie / dataset
            'data': list(map(lambda x: x[index], datas[1:]))
        })

    bkg = '%23191919' if dark_mode else 'white'
    data = {
        'type': chart_type.lower().replace('chart', ''),
        'data': {
            'labels': labels,
            'borderWidth': 0,
            'datasets': datasets
        },
        'options': {
            **(force_white_labels if dark_mode else {}),
            'plugins': {'outlabels': {'text': ''}},
            'rotation': 0,
        }
    }

    if request.headers.get('sec-ch-prefers-color-scheme') == 'dark':
        dark_mode = True

    resp = redirect(CHART_URL + f'&bkg={bkg}&c=' + json.dumps(data))
    resp.headers['Vary'] = 'Sec-CH-Prefers-Color-Scheme'
    resp.headers['Accept-CH'] = 'Sec-CH-Prefers-Color-Scheme'
    resp.headers['Critical-CH'] = 'Sec-CH-Prefers-Color-Scheme'
    return resp


if __name__ == '__main__':
    app.run()
