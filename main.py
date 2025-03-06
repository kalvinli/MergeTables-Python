from flask import Flask, request, render_template, url_for, redirect, jsonify
import json,requests
from concurrent.futures import ThreadPoolExecutor
from base_class.base_func import batch_merge_bases_func


executor = ThreadPoolExecutor()
app = Flask(__name__, static_folder="static", static_url_path="/static")
app.config['JSON_AS_ASCII'] = False

webhook_url = ""

## 前端用户界面，通过前端SDK操作，后续不再更新，仅用作生成后端参数用
@app.route('/', methods=['GET'])
def index():
  return render_template('index.html')

## 一键关联用户界面，通过前端SDK操作
@app.route('/onekeylink', methods=['GET'])
def onekeylink():
  return render_template('onekeylink.html')

@app.route('/favicon.ico')
def favicon_ico():
    return redirect(url_for('static', filename='favicon.ico'))

@app.route('/favicon.svg')
def favicon_svg():
    return redirect(url_for('static', filename='favicon.svg'))

# @app.route('/meta.json')
# def meta():
#     return redirect(url_for('static', filename='meta.json'))

## 异步任务执行完成后通过此函数回调，通过配置 webhook_url 将执行结果返回给用户
def return_result(future):
    result = future.result()
    print("Task result:", result)
    if webhook_url is not None:
      requests.post(url=webhook_url, json={"result": result})

## 自动化流程POST接口，通过后端SDK操作
@app.route('/batch_merge_tables', methods=['POST'])
def batch_merge_tables():

  data = request.get_data().decode()
  # print(data)

  try:
    data_new = data.replace("\r\n", "")
    data_new = data_new.replace("\n", "")
    while (", " in data_new):
      data_new = data_new.replace(", ", ",")
    
    data = data_new.replace(",]", "]")
    data = data.replace(",}", "}")
    # print(data)
    data_post_json = json.loads(data)
  except Exception as e:
    return "参数错误"
  
  if data_post_json == '' or data_post_json == {}:
    return "参数为空"

  
  merge_type = request.args.get("type","")
  if merge_type == "":
    merge_type = data_post_json.get("type", "")

  record_id = request.args.get("record_id","")
  if record_id != "":
    data_post_json["record_id"] = record_id

  global webhook_url
  webhook_url = request.args.get("webhook_url", None)

  # print(data_post_json)

  code = 200

  if merge_type == "async":
    executor.submit(batch_merge_bases_func, data_post_json).add_done_callback(return_result)

    result = "后台正在处理数据中，请稍后检查目标表中的数据"
    return {"code": code, "msg": result}
  else:
    result = batch_merge_bases_func(data_post_json)

  # print(result)
  return {"code": code, "msg": result}


app.run(host='0.0.0.0', port=3300, debug=False, use_reloader=True)
