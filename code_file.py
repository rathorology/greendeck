import json
from flask_cors import CORS
from flask import request
import gdown
import flask
import os
import pandas as pd
import numpy as np
import operator

url = 'https://drive.google.com/a/greendeck.co/uc?id=19r_vn0vuvHpE-rJpFHvXHlMvxa8UOeom&export=download'
# Create Flask application
app = flask.Flask(__name__)
CORS(app)


# Converts operator symbol to function
def get_operator_fn(op):
    return {
        '<': operator.lt,
        '>': operator.gt,
        '==': operator.eq,
    }[op]


# Will be used across all problem statement to load pre-process data and apply condition.
def preprocesser(filters):
    df = pd.read_csv('dumps/nap.csv')

    df['discount'] = 100 * ((df['price_regular_price'] - df['price_offer_price']) / df['price_regular_price'])
    if filters == None:
        return df
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
            df = df[df[['diss5da94f4e6d97010001f81d72', 'diss5da94f270ffeca000172b12e',
                        'diss5d0cc7b68a66a100014acdb0', 'diss5da94ef80ffeca000172b12c',
                        'diss5da94e940ffeca000172b12a']].any(1)]
        elif operand1 == 'competition':
            df = df[df['diss' + operand2] == True]
        else:
            print("New Condition")

    return df


# Problem statement 1
def discounted_products_list(filters):
    df = preprocesser(filters)
    discounted_products_list = df['_id'].tolist()
    return {'discounted_products_list': discounted_products_list}


# Problem statement 2
def discounted_products_count_or_avg_discount(query, filters):
    df = preprocesser(filters)
    discounted_products_count = df.shape[0]
    avg_dicount = np.average(df['discount'])
    if query == "discounted_products_count|avg_discount":
        return {'discounted_products_count': discounted_products_count, 'avg_discount': avg_dicount}
    elif query == "discounted_products_count":
        return {'discounted_products_count': discounted_products_count}
    else:
        return {'avg_discount': avg_dicount}


# Problem statement 3
def expensive_list(filters):
    df = preprocesser(filters)
    df = df[(df['price_basket_price'] > df['5da94f4e6d97010001f81d72']) |
            (df['price_basket_price'] > df['5da94f270ffeca000172b12e']) |
            (df['price_basket_price'] > df['5d0cc7b68a66a100014acdb0']) |
            (df['price_basket_price'] > df['5da94ef80ffeca000172b12c']) |
            (df['price_basket_price'] > df['5da94e940ffeca000172b12a'])]

    expensive_list = df['_id'].tolist()
    return {'expensive_list': expensive_list}


# Problem staement 4
def competition_discount_diff_list(filters):
    df = preprocesser(filters)
    competition_discount_diff_list = df['_id'].tolist()
    return {"competition_discount_diff_list": competition_discount_diff_list}


# Download data
def init_files(dump_path='dumps/netaporter_gb.json'):
    if dump_path.split('/')[0] not in os.listdir():
        os.mkdir(dump_path.split('/')[0])
    if os.path.exists(dump_path):
        pass
    else:
        gdown.download(url=url, output=dump_path, quiet=False)


# Preprocess data from json file and dump csv into dumps
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


# Main Function
@app.route('/master_function', methods=['POST'])
def master_function():
    args = request.get_json(force=True)
    query = args['query_type']
    try:
        filters = args['filters']
    except Exception as e:
        filters = None
    if query == 'discounted_products_list':
        results = discounted_products_list(filters)
    elif query in ['discounted_products_count', 'avg_discount', 'discounted_products_count|avg_discount']:
        results = discounted_products_count_or_avg_discount(query, filters)
    elif query == 'expensive_list':
        results = expensive_list(filters)
    elif query == 'competition_discount_diff_list':
        results = competition_discount_diff_list(filters)
    else:
        results = 'Invalid Query'
    return results


# RUN FLASK APPLICATION
if __name__ == '__main__':
    # RUNNNING FLASK APP
    app.run(debug=True, threaded=True, host='0.0.0.0', port=5000)
