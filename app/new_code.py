import json
from functools import partial
from flask_cors import CORS
from flask import request
import gdown
import flask
import io
import os

url = 'https://drive.google.com/a/greendeck.co/uc?id=19r_vn0vuvHpE-rJpFHvXHlMvxa8UOeom&export=download'
# Create Flask application
app = flask.Flask(__name__)
CORS(app)

import pandas as pd
import numpy as np
import operator


def get_operator_fn(op):
    return {
        '<': operator.lt,
        '>': operator.gt,
        '==': operator.eq,
    }[op]


def preprocesser(filters):
    df = pd.read_csv('NAP.csv')
    # df = df[['_id', 'name', 'price_offer_price', 'price_regular_price']]

    df['discount'] = 100 * ((df['price_regular_price'] - df['price_offer_price']) / df['price_regular_price'])
    for cond in filters:
        operand1 = cond['operand1']
        op = cond['operator']
        operand2 = cond['operand2']
        if operand1 == 'discount':
            df = df[get_operator_fn(op)(df['discount'], operand2)]
        elif operand1 == 'brand.name':
            df = df[df['brand'] == operand2]
        elif operand1 == 'discount_diff':
            comp_id = ['5da94f4e6d97010001f81d72', '5da94f270ffeca000172b12e', '5d0cc7b68a66a100014acdb0',
                       '5da94ef80ffeca000172b12c', '5da94e940ffeca000172b12a']
            for id in comp_id:
                df['diss' + str(id)] = (df['price_basket_price'] - df[id]) > (df[id] * (operand2 / 100))
        elif operand1 == 'competition':
            df = df[df['diss' + operand2] == True]
        else:
            print("New Condition")

        return df
    return df


def discounted_products_list(filters, inner):
    df = preprocesser(filters)
    discounted_products_list = df['_id'].tolist()
    return {'discounted_products_list': discounted_products_list}


def discounted_products_count_or_avg_discount(query, filters):
    df = preprocesser(filters)
    discounted_products_count = df.shape[0]
    avg_dicount = np.average(df['discount'])
    if query == "discounted_products_count|avg_discount":
        return {'discounted_products_count': discounted_products_count, 'avg_dicount': avg_dicount}
    elif query == "discounted_products_count":
        return {'discounted_products_count': discounted_products_count}
    else:
        return {'avg_dicount': avg_dicount}


def expensive_list(filters):
    df = preprocesser(filters)
    df = df[(df['price_basket_price'] > df['5da94f4e6d97010001f81d72']) |
            (df['price_basket_price'] > df['5da94f270ffeca000172b12e']) |
            (df['price_basket_price'] > df['5d0cc7b68a66a100014acdb0']) |
            (df['price_basket_price'] > df['5da94ef80ffeca000172b12c']) |
            (df['price_basket_price'] > df['5da94e940ffeca000172b12a'])]

    expensive_list = df['_id'].tolist()
    return {'expensive_list': expensive_list}


def competition_discount_diff_list(filters):
    df = preprocesser(filters)
    competition_discount_diff_list = df['_id'].tolist()
    return {"competition_discount_diff_list": competition_discount_diff_list}


# filters = [{"operand1": "discount", "operator": ">", "operand2": 5},
#            {"operand1": "brand.name", "operator": "==", "operand2": "balenciaga"}]
# result = discounted_products_list(filters,inner=False)
# result = discounted_products_list(filters, inner=True)
########################################################################################
# query = "discounted_products_count|avg_discount"
# filters = [{"operand1": "brand.name", "operator": "==", "operand2": "balenciaga"},
#            {"operand1": "discount", "operator": ">", "operand2": 10}]
# result = discounted_products_count_or_avg_discount(query, filters)
###########################################################################################
# filters = [{"operand1": "brand.name", "operator": "==", "operand2": "gucci"}]
# result = expensive_list(filters)
# print(result)
###########################################################################################
# filters = [
#     {"operand1": "discount_diff", "operator": ">", "operand2": 10},
#     {"operand1": "competition", "operator": "==", "operand2": "5d0cc7b68a66a100014acdb0"}
# ]
# result = competition_discount_diff_list(filters)

def init_files(dump_path='dumps/netaporter_gb.json'):
    if dump_path.split('/')[0] not in os.listdir():
        os.mkdir(dump_path.split('/')[0])
    if os.path.exists(dump_path):
        pass
    else:
        gdown.download(url=url, output=dump_path, quiet=False)


def prepare_dataset(path='dumps/netaporter_gb.json'):
    '''YOUR DATA PREPARATION CODE HERE'''
    csv_path = 'dumps/nap.csv'
    if os.path.exists(csv_path):
        pass
    else:
        product_json = []
        with open(path) as fp:
            for product in fp.readlines():
                product_json.append(json.loads(product))
        df = pd.DataFrame(product_json)
        print(df.applymap(lambda x: isinstance(x, dict)).all())
        df['_id'] = df['_id'].apply(pd.Series)
        df['brand'] = df['brand'].apply(pd.Series)['name']
        df['website_id'] = df['website_id'].apply(pd.Series)
        df['price_offer_price'] = df['price'].apply(pd.Series)['offer_price'].apply(pd.Series)['value']
        df['price_regular_price'] = df['price'].apply(pd.Series)['regular_price'].apply(pd.Series)['value']
        df['price_basket_price'] = df['price'].apply(pd.Series)['basket_price'].apply(pd.Series)['value']

        df['5da94f4e6d97010001f81d72'] = df['similar_products'].apply(pd.Series)['website_results'].apply(pd.Series)[
            '5da94f4e6d97010001f81d72']
        df['5da94f270ffeca000172b12e'] = df['similar_products'].apply(pd.Series)['website_results'].apply(pd.Series)[
            '5da94f270ffeca000172b12e']
        df['5d0cc7b68a66a100014acdb0'] = df['similar_products'].apply(pd.Series)['website_results'].apply(pd.Series)[
            '5d0cc7b68a66a100014acdb0']
        df['5da94ef80ffeca000172b12c'] = df['similar_products'].apply(pd.Series)['website_results'].apply(pd.Series)[
            '5da94ef80ffeca000172b12c']
        df['5da94e940ffeca000172b12a'] = df['similar_products'].apply(pd.Series)['website_results'].apply(pd.Series)[
            '5da94e940ffeca000172b12a']
        comp_id = ['5da94f4e6d97010001f81d72', '5da94f270ffeca000172b12e', '5d0cc7b68a66a100014acdb0',
                   '5da94ef80ffeca000172b12c', '5da94e940ffeca000172b12a']
        for i in comp_id:
            val = list()
            for j in df[i]:
                try:
                    print(j['knn_items'][0]['_source']['price']['basket_price']['value'])
                    val.append(j['knn_items'][0]['_source']['price']['basket_price']['value'])
                except Exception as e:
                    val.append(np.nan)
            df[i] = val
        drop_col = ['sku', 'name', 'url', 'media', 'meta', 'price', 'description_text', 'spider', 'stock',
                    'classification', 'created_at', 'updated_at', 'similar_products', 'positioning', 'lv_url',
                    'price_changes', 'price_positioning', 'price_positioning_text', 'sizes']
        df = df.drop(drop_col, axis=1)
        df.to_csv("dumps/nap.csv", index=False)
def master_function(args):
    query = args['query_type']
    filters = args['filters']

    return results

# RUN FLASK APPLICATION
if __name__ == '__main__':
    '''MAKE SURE YOU HAVE 'gdown' LIBRARY IN YOUR 'requirements.txt' TO DOWNLOAD FILE FROM Gdrive.'''
    # GETTING DATASET this function will download the dataset
    init_files('dumps/netaporter_gb.json')

    # PREPARING DATASET
    prepare_dataset('dumps/netaporter_gb.json')

    # master_function(args)

    # RUNNNING FLASK APP
    app.run(debug=True, host='0.0.0.0', port=5000)
