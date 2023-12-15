from flask import Flask, request, jsonify
import pandas as pd
import numpy as np
import mysql.connector

app = Flask(__name__)

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="servall1"
)


@app.route('/purchase_order', methods=['GET'])
def purchase_order_process():
    if request.method == 'GET':
        try:
            garage_id = int(request.args.get('garage_id'))
        except Exception  as e:
            return jsonify({'error': f"{e}",'status': '400'})
        if garage_id:
            df_purchaseorder = pd.read_sql('SELECT * FROM purchase_orders', con=mydb)
            df_purchaseparts = pd.read_sql('SELECT * FROM purchase_parts', con=mydb)
            df_parts = pd.read_sql('SELECT * FROM parts', con=mydb)
            parts_name = df_parts.merge(df_purchaseparts, left_on='id', right_on='parts_id').merge(df_purchaseorder, left_on='purchase_id', right_on='id')
            combine_value = parts_name[['name','qty','rate','amount','tax','discount_amount','garage_id','created_at']]
            combine_value = combine_value[combine_value['garage_id'] == int(garage_id)]
            combine_value['created_at'] = pd.to_datetime(combine_value['created_at'])
            combine_value['month'] = combine_value['created_at'].dt.to_period('M')
            result = combine_value.groupby(['name','month'])['qty'].sum().unstack('month').sort_values(['name'],ascending=True).replace(np.nan, 0).reset_index()
            json_records = result.to_json(orient='records')
            if json_records != "[]":
                return json_records
            else:
                return jsonify({'error':"result list is empty",'status': '404'})
        return jsonify({'error':"Error Garage with id not found!",'status': '404'})


if __name__ == '__main__':
    app.run()