from baseopensdk.api.base.v1 import *
from baseopensdk import BaseClient
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, ALL_COMPLETED
import os, json, math, re, time
import requests
 

MESSAGE_LIST = []

cpu_count = os.cpu_count()
if cpu_count > 1:
  thread_num = math.ceil(cpu_count / 2)
else:
  thread_num = 1
  

###########################   判断字符串前后是否包含{{}}   ###########################
def is_test(string):
    # 判断是否为映射字段模式，^开头$结尾，中间包含两对大括号{}
    pattern = r'^\{\{.*\}\}$'
    if re.match(pattern, string):
      return True
    else:
      return False


###########################   批量合并多个多维表格中数据表的数据   ###########################
def batch_merge_bases_func(data_post_json: str):
  # print(data_post_json, flush=True)

  data_json = data_post_json.get("parameters","")
  if data_json == '':
    return "参数为空"
  
  # print("=" * 30)
  
  executor = ThreadPoolExecutor(max_workers = 8)

  global MESSAGE_LIST
  MESSAGE_LIST = []
  MESSAGE_LIST.append("======================================")
  
  TARGET_APP_TOKEN = data_json.get("app_token")
  TARGET_PERSONAL_BASE_TOKEN = data_json.get("personal_base_token")

  SOURCE_BASE_LIST = data_json.get("source_base_list", [])
  SELECT_VIEW = data_json.get("select_view")
  TABLE_TARGET = data_json.get("table_Target")
  FIELD_TARGET = data_json.get("field_Target")
  FIELD_TARGET_META = data_json.get("field_Target_Meta")
  FATHER_FIELD_TARGET = data_json.get("father_field_Target",[])
  INDEX_FIELD = data_json.get("index_field", [])

  CHKECKBOX_CLEAN_TABLE = data_json.get("checkbox_clean_table", [])
  SYNC_OPTIONS_TARGET = data_json.get("sync_options_target")

  FIELDS_DEFAULTVALUE = data_post_json.get("fields_defaultValue",{})
  DEFAULTVALUE_MAPPING_VALUE = {}
  DEFAULTVALUE_MAPPING_MODE = ""
  DEFAULTVALUE_UPDATING_POLICY = "retain"
  # DEFAULTVALUE_UPDATING_POLICY = "clear"
  if FIELDS_DEFAULTVALUE != {}:
    DEFAULTVALUE_MAPPING_VALUE = FIELDS_DEFAULTVALUE.get("mapping_value", {})
    DEFAULTVALUE_MAPPING_MODE = FIELDS_DEFAULTVALUE.get("mapping_mode", "").lower() # both|new|update
    DEFAULTVALUE_UPDATING_POLICY = FIELDS_DEFAULTVALUE.get("updating_policy", "retain").lower() # clear|retain


  # 如果参数中的"SOURCE_BASE_LIST"为[]时，则通过参数进行生成
  if SOURCE_BASE_LIST == []:
    SOURCE_BASE_LIST_TMP = {}
    SOURCE_BASE_LIST_TMP["base_id"] = TARGET_APP_TOKEN
    SOURCE_BASE_LIST_TMP["table_list"] = data_json.get("table_Source_List")
    SOURCE_BASE_LIST_TMP["base_token"] = TARGET_PERSONAL_BASE_TOKEN
    SOURCE_BASE_LIST_TMP["table_select_auto"] = False
    SOURCE_BASE_LIST_TMP["table_select_keyword"] = ""
    SOURCE_BASE_LIST.append(SOURCE_BASE_LIST_TMP)

  # 当前任务开始时间
  start_timestamp = time.time()
  local_start_time = time.localtime()
  formatted_local_start_time = time.strftime('%Y-%m-%d %H:%M:%S', local_start_time)
  time_format = "开始时间：" + formatted_local_start_time
  print(time_format)
  MESSAGE_LIST.append(time_format)

  RECORD_ID = data_post_json.get("record_id", None)
  if RECORD_ID is not None:
    if len(SOURCE_BASE_LIST) > 1 or len(SOURCE_BASE_LIST[0].get("table_list", [])) > 1:
      result = "指定单条记录ID后，源多维表格或源数据表不能设置为多个"
      return result

  # print(SOURCE_BASE_LIST)
  # print(SOURCE_BASE_LIST[0].get("tables_select_auto") is False)

  # 遍历目标表整表数据，并写入索引字段到数组
  # 构建目标表client
  target_client: BaseClient = BaseClient.builder() \
    .app_token(TARGET_APP_TOKEN) \
    .personal_base_token(TARGET_PERSONAL_BASE_TOKEN) \
    .build()
  

  ########### 若不存在字段，则新建，若存在则获取字段 ID #############
  list_field_request = ListAppTableFieldRequest().builder() \
    .table_id(TABLE_TARGET) \
    .build()
  
  list_field_response = target_client.base.v1.app_table_field.list(
        list_field_request)
  # print(list_field_response.data.__dict__)

  field_items = getattr(list_field_response.data, 'items', [])

  field_list = {}
  for field_item in field_items:
    field_name = field_item.field_name
    field_id = field_item.field_id
    field_list[field_name] = field_id

  # 创建源表名字段 Source Table
   # 源表名
  SOURCE_TABLE = "Source Table"
  field_id = field_list.get(SOURCE_TABLE, None)
  if field_id is None:
    MESSAGE_LIST.append("在目标数据表创建【" + SOURCE_TABLE + "】字段")
    try:
      create_field_request = CreateAppTableFieldRequest().builder() \
        .table_id(TABLE_TARGET) \
        .request_body(
          AppTableField.builder() \
            .field_name(SOURCE_TABLE) \
            .type(1) \
            .build()
        ) \
        .build()
      
      create_field_response = target_client.base.v1.app_table_field.create(
            create_field_request)
      # print(create_field_response.__dict__)

    except Exception as e:
      pass

  # 创建源记录ID字段 Source Record ID
  # 源记录ID
  SOURCE_RECORD_ID = "Source Record ID"
  SOURCE_RECORD_ID_FIELD_ID = ""
  field_id = field_list.get(SOURCE_RECORD_ID, None)
  if field_id is None:
    MESSAGE_LIST.append("在目标数据表创建【" + SOURCE_RECORD_ID + "】字段")
    try:
      create_field_request = CreateAppTableFieldRequest().builder() \
        .table_id(TABLE_TARGET) \
        .request_body(
          AppTableField.builder() \
            .field_name(SOURCE_RECORD_ID) \
            .type(1) \
            .build()
        ) \
        .build()
      
      create_field_response = target_client.base.v1.app_table_field.create(
            create_field_request)
        
      # print(create_field_response.__dict__)
      SOURCE_RECORD_ID_FIELD_ID = create_field_response.data.field.field_id

    except Exception as e:
      pass

  else:
    SOURCE_RECORD_ID_FIELD_ID = field_id

  # print(SOURCE_RECORD_ID, ":", SOURCE_RECORD_ID_FIELD_ID)

  # 如果父记录字段不为空，则创建源记录父记录ID字段、目标表记录ID和目标表记录父记录ID
  if len(FATHER_FIELD_TARGET) > 0:
    
    # 创建源记录父记录ID字段 Source Parent ID
    # 源记录父记录ID
    SOURCE_PARENT_FIELD = "Source Parent ID"
    SOURCE_PARENT_FIELD_ID = ""
    field_id = field_list.get(SOURCE_PARENT_FIELD, None)
    if field_id is None:
      MESSAGE_LIST.append("在目标数据表创建【" + SOURCE_PARENT_FIELD + "】字段")
      try:
        create_field_request = CreateAppTableFieldRequest().builder() \
          .table_id(TABLE_TARGET) \
          .request_body(
            AppTableField.builder() \
              .field_name(SOURCE_PARENT_FIELD) \
              .type(1) \
              .build()
          ) \
          .build()
        
        create_field_response = target_client.base.v1.app_table_field.create(
              create_field_request)
        # print(create_field_response.__dict__)
        SOURCE_PARENT_FIELD_ID = create_field_response.data.field.field_id

      except Exception as e:
        pass

    else:
      SOURCE_PARENT_FIELD_ID = field_id

    # print(SOURCE_PARENT_FIELD, ":", SOURCE_PARENT_FIELD_ID)


    # 创建目录表记录ID字段 Record ID
    # 目标表记录ID
    TARGET_RECORD_ID = "Record ID"
    RECORD_ID_FIELD_ID = ""
    field_id = field_list.get(TARGET_RECORD_ID, None)
    if field_id is None:
      MESSAGE_LIST.append("在目标数据表创建【" + TARGET_RECORD_ID + "】字段")
      try:
        create_field_request = CreateAppTableFieldRequest().builder() \
          .table_id(TABLE_TARGET) \
          .request_body(
            AppTableField.builder() \
              .field_name(TARGET_RECORD_ID) \
              .type(20) \
              .property({"formula_expression": "RECORD_ID()"}) \
              .build()
          ) \
          .build()
        
        create_field_response = target_client.base.v1.app_table_field.create(
              create_field_request)
        # print(create_field_response.data.field.__dict__)
        RECORD_ID_FIELD_ID = create_field_response.data.field.field_id

      except Exception as e:
        pass

    else:
      RECORD_ID_FIELD_ID = field_id

    # print(TARGET_RECORD_ID, ":", RECORD_ID_FIELD_ID)

    # 创建目标表父记录ID字段 Parent Record ID
    # 目标表记录父记录ID
    TARGET_PARENT_RECORD_ID = "Parent Record ID"
    PARENT_RECORD_ID_FIELD_ID = ""
    formula_expression = "bitable::$table["+TABLE_TARGET+"].FILTER(CurrentValue.$column["+SOURCE_RECORD_ID_FIELD_ID+"]=bitable::$table["+TABLE_TARGET+"].$field["+SOURCE_PARENT_FIELD_ID+"]).$column["+RECORD_ID_FIELD_ID+"]"
    # print(formula_expression)
    field_id = field_list.get(TARGET_PARENT_RECORD_ID, None)

    # if field_id is not None:
    #   delete_field_request = DeleteAppTableFieldRequest().builder() \
    #     .table_id(TABLE_TARGET) \
    #     .field_id(field_id) \
    #     .build()
      
    #   delete_field_response = target_client.base.v1.app_table_field.delete(
    #         delete_field_request)
    
    if field_id is None:
      MESSAGE_LIST.append("在目标数据表创建【" + TARGET_PARENT_RECORD_ID + "】字段")
      try:
        create_field_request = CreateAppTableFieldRequest().builder() \
          .table_id(TABLE_TARGET) \
          .request_body(
            AppTableField.builder() \
              .field_name(TARGET_PARENT_RECORD_ID) \
              .type(20) \
              .property({"formula_expression": formula_expression}) \
              
              .build()
          ) \
          .build()
        
        create_field_response = target_client.base.v1.app_table_field.create(
              create_field_request)
        # print(create_field_response.__dict__)

        PARENT_RECORD_ID_FIELD_ID = create_field_response.data.field.field_id

      except Exception as e:
        pass
    
    else:
      PARENT_RECORD_ID_FIELD_ID = field_id

    
    # print(TARGET_PARENT_RECORD_ID, ":", PARENT_RECORD_ID_FIELD_ID)

  # 在目标数据表中创建索引编号字段【Index Number】和【Index Number（Formula）】
  INDEX_NUMBER = "Index Number"
  INDEX_NUMBER_FORMULA = "Index Number（Formula）"
  field_id = field_list.get("Index Number", None)
  index_field_id = field_list.get("Index Number（Formula）", None)
  INDEX_NUMBER_FIELD_ID = ""
  if field_id is None or index_field_id is None:
    try:
      create_field_request = CreateAppTableFieldRequest().builder() \
        .table_id(TABLE_TARGET) \
        .request_body(
          AppTableField.builder() \
            .field_name(INDEX_NUMBER) \
            .type(1005) \
            .build()
        ) \
        .build()
      
      create_field_response = target_client.base.v1.app_table_field.create(
            create_field_request)

      INDEX_NUMBER_FIELD_ID = create_field_response.data.field.field_id
      print("在目标数据表创建【" + INDEX_NUMBER + "】字段")
      MESSAGE_LIST.append("在目标数据表创建【" + INDEX_NUMBER + "】字段")

    except Exception as e:
      INDEX_NUMBER_FIELD_ID = field_id

    formula_expression = "bitable::$table["+TABLE_TARGET+"].COUNTIF(CurrentValue.$column["+INDEX_NUMBER_FIELD_ID+"]<=bitable::$table["+TABLE_TARGET+"].$field["+INDEX_NUMBER_FIELD_ID+"])"
    try:
      create_field_request = CreateAppTableFieldRequest().builder() \
        .table_id(TABLE_TARGET) \
        .request_body(
          AppTableField.builder() \
            .field_name(INDEX_NUMBER_FORMULA) \
            .type(20) \
            .property({"formula_expression": formula_expression}) \
            .build()
        ) \
        .build()
      
      data_status = True
      while data_status:
        create_field_response = target_client.base.v1.app_table_field.create(
            create_field_request)
        if create_field_response.code == 0:
          data_status = False
      if create_field_response.code == 1254014:
        raise "【" + INDEX_NUMBER_FORMULA + "】字段已存在"
      else:
        print("在目标数据表创建【" + INDEX_NUMBER_FORMULA + "】字段")
        MESSAGE_LIST.append("在目标数据表创建【" + INDEX_NUMBER_FORMULA + "】字段")

    except Exception as e:
      update_field_request = UpdateAppTableFieldRequest().builder() \
        .table_id(TABLE_TARGET) \
        .field_id(index_field_id) \
        .request_body(
          AppTableField.builder() \
            .field_name(INDEX_NUMBER_FORMULA) \
            .type(20) \
            .property({"formula_expression": formula_expression}) \
            .build()
        ) \
        .build()
      
      update_field_response = target_client.base.v1.app_table_field.update(
            update_field_request)
      print("在目标数据表更新【" + INDEX_NUMBER_FORMULA + "】字段")
      MESSAGE_LIST.append("在目标数据表更新【" + INDEX_NUMBER_FORMULA + "】字段")
      

  FIELD_NAMES = [SOURCE_TABLE, SOURCE_RECORD_ID]
  page_token = ""
  search_records_resp = ""
  resp_code = ""
  while resp_code != 0:
    search_records_resp = search_records(TARGET_APP_TOKEN, TARGET_PERSONAL_BASE_TOKEN, TABLE_TARGET, '', page_token, None, FIELD_NAMES)
    if search_records_resp.get("code") == 0:
      resp_code = 0

  total = search_records_resp.get("data").get("total")
  # filter_info = {
  #                 "conjunction": "and",
  #                 "conditions": [
  #                   {
  #                     "field_name": INDEX_NUMBER_FORMULA,
  #                     "operator": "is",
  #                     "value": [total]
  #                   }
  #                 ]
  #               }
  
  # resp_code = ""
  # while resp_code != 0:
  #   search_records_resp = search_records(TARGET_APP_TOKEN, TARGET_PERSONAL_BASE_TOKEN, TABLE_TARGET, '', page_token, filter_info, FIELD_NAMES)
  #   if search_records_resp.get("code") == 0:
  #     resp_code = 0

  print("目标数据表共有 " + str(total) + " 条记录")
  MESSAGE_LIST.append("目标数据表共有 " + str(total) + " 条记录")
    

  # 如果索引字段不为空，则遍历目标数据表获取所有的【Source Record ID】和记录ID，用于与源表记录进行比对
  target_record_id_map = {}
  INDEX_FIELD_NAME = ""
  if len(INDEX_FIELD) > 0:
    MESSAGE_LIST.append("获取目标数据表数据列表")
    page_token = ""
    INDEX_FIELD_NAME = INDEX_FIELD[0]
    FIELD_NAMES = [INDEX_FIELD_NAME]
    step = 500
    filter_info_list = []
    future_list = []
    for i in range(0, total, step):
      last_num = min(i + step, total)
      # print(str(i + 1) + " ~ " + str(last_num))
      filter_info = {
                      "conjunction": "and",
                      "conditions": [
                        {
                          "field_name": INDEX_NUMBER_FORMULA,
                          "operator": "isGreaterEqual",
                          "value": [i + 1]
                        },
                        {
                          "field_name": INDEX_NUMBER_FORMULA,
                          "operator": "isLessEqual",
                          "value": [last_num]
                        }
                      ]
                    }
      
      filter_info_list.append(filter_info)
      if len(filter_info_list) == 20 or last_num == total:
        # print(filter_info_list)

        with ThreadPoolExecutor(max_workers=4) as search_executor: 
          for index in range(len(filter_info_list)):
            future = search_executor.submit(search_records, TARGET_APP_TOKEN, TARGET_PERSONAL_BASE_TOKEN, TABLE_TARGET, '', page_token, filter_info_list[index], FIELD_NAMES)
            future_list.append(future)

        for future_item in future_list:
          future_item.result()
          record_items = future_item.result().get("data").get("items")
          for record_item in record_items:
            record_id = record_item.get("record_id")
            FIELD_INDEX_VALUE = record_item.get("fields").get(INDEX_FIELD_NAME, None)
            if FIELD_INDEX_VALUE is not None:
              FIELD_INDEX_VALUE = FIELD_INDEX_VALUE[0].get("text")
              target_record_id_map[FIELD_INDEX_VALUE] = record_id

    
    MESSAGE_LIST.append("目标数据表数据映射关系生成完成")
    # print(target_record_id_map)
    # print(len(target_record_id_map))


  # 遍历目标数据表获取所有的记录ID，如果选择了索引字段，则不执行删除
  if len(CHKECKBOX_CLEAN_TABLE) > 0 and len(INDEX_FIELD) == 0:
    print("获取目标数据表数据并全部删除")
    MESSAGE_LIST.append("获取目标数据表数据并全部删除")
    page_token = ""
    FIELD_NAMES = [SOURCE_RECORD_ID]
    delete_total = 0
    step = 500
    filter_info_list = []
    target_record_list = []
    target_recordid_list = []
    future_list = []
    recordid_future_list = []
    # for i in range(1, total, step):
    #   last_num = min(i + step, total + 1) - 1
    for i in range(total, 0, -step):
      begin_num = max(0, i - step)
      # print("正在请求 " + str(begin_num) + " ~ " + str(i) + " 行记录")
      # MESSAGE_LIST.append("正在请求 " + str(begin_num) + " ~ " + str(i) + " 行记录")
      filter_info = {
                      "conjunction": "and",
                      "conditions": [
                        {
                          "field_name": INDEX_NUMBER_FORMULA,
                          "operator": "isGreaterEqual",
                          "value": [begin_num + 1]
                        },
                        {
                          "field_name": INDEX_NUMBER_FORMULA,
                          "operator": "isLessEqual",
                          "value": [i]
                        }
                      ]
                    }
      filter_info_list.append(filter_info)
      if len(filter_info_list) == 40 or begin_num == 0:
        # print(filter_info_list)

        with ThreadPoolExecutor(max_workers=4) as search_executor: 
          for filter_info_item in filter_info_list:
            future = search_executor.submit(search_records, TARGET_APP_TOKEN, TARGET_PERSONAL_BASE_TOKEN, TABLE_TARGET, '', page_token, filter_info_item, FIELD_NAMES)
            future_list.append(future)
        # print(len(future_list))
        for future_item in as_completed(future_list):
          if future_item.done():
            # print(future_item.done())
            future_item.result()
            try:
              record_items = future_item.result().get("data").get("items")
              delete_total += len(record_items)
            
              print("正在读取 " + str(delete_total - len(record_items) + 1) + " ~ " + str(delete_total) + " 行记录")
              # MESSAGE_LIST.append("正在读取 " + str(delete_total - len(record_items) + 1) + " ~ " + str(delete_total) + " 行记录")
              
              for record_item in record_items:
                record_id = record_item.get("record_id")
                target_record_list.append(record_id)
            except Exception as e:
              continue

            target_recordid_list.append(target_record_list)
            target_record_list = []

        with ThreadPoolExecutor(max_workers=4) as delete_executor:
          for recordid_future_item in target_recordid_list:
            recordid_future = delete_executor.submit(batch_delete_records, TARGET_APP_TOKEN, TARGET_PERSONAL_BASE_TOKEN, TABLE_TARGET, recordid_future_item)
 
        filter_info_list = []
        future_list = []
        target_recordid_list = []


    total = 0
    result = "总共删除 " + str(delete_total) + " 条记录"
    print(result)
    MESSAGE_LIST.append(result)

  # 当前任务结束时间
  end_timestamp = time.time() 
  local_end_time = time.localtime()    
  formatted_local_end_time = time.strftime('%Y-%m-%d %H:%M:%S', local_end_time)
  time_format = "结束时间：" + formatted_local_end_time
  print(time_format)
  MESSAGE_LIST.append(time_format)
  if end_timestamp - start_timestamp >= 60:
    time_format = "执行时长：" + str(math.floor((end_timestamp - start_timestamp)/60)) + " 分 " + str(math.floor((end_timestamp - start_timestamp) % 60)) + " 秒"
    print(time_format)
    MESSAGE_LIST.append(time_format)
  else:
    time_format = "执行时长：" + str(math.floor(end_timestamp - start_timestamp)) + " 秒"
    print(time_format)
    MESSAGE_LIST.append(time_format)

  MESSAGE_LIST.append("====================")
  # print("\r\n".join(MESSAGE_LIST))
  # return ""



  ###########################################################################
  ##                            遍历所有的源多维表格                         ##
  ###########################################################################
  total = 0
  total_create = 0
  total_update = 0
  table_name_list = {}
  for base_item in SOURCE_BASE_LIST:
    # print(base_item)
    APP_TOKEN = base_item.get("base_id")
    PERSONAL_BASE_TOKEN = base_item.get("base_token")
    TABLE_SOURCE_LIST = base_item.get("table_list")
    TABLE_SELECT_AUTO = base_item.get("table_select_auto", False)
    TABLE_SELECT_KEYWORD = base_item.get("table_select_keyword", "")


    # 构建源表client
    client: BaseClient = BaseClient.builder() \
        .app_token(APP_TOKEN) \
        .personal_base_token(PERSONAL_BASE_TOKEN) \
        .build()
    
    # 获取源多维表格中所有数据表并生成新的json对象table_name_list
    # 如果设置了 table_select_auto 和 table_select_keyword，会自动根据参数动态获取数据表
    list_table_request = ListAppTableRequest.builder() \
      .page_size(500) \
      .app_token(APP_TOKEN) \
      .build()
    
    try:
      list_table_response = client.base.v1.app_table.list(
          list_table_request)
    except Exception as e:
      time.sleep(2)
      list_table_response = client.base.v1.app_table.list(
          list_table_request)
    table_list = getattr(list_table_response.data, 'items', [])
    if TABLE_SELECT_AUTO is True and TABLE_SELECT_KEYWORD != "":
      TABLE_SOURCE_LIST = []

      for table_item in table_list:
        table_name_list[table_item.table_id] = table_item.name

        if TABLE_SELECT_KEYWORD in table_item.name:
          TABLE_SOURCE_LIST.append(table_item.table_id)

    else:
      for table_item in table_list:
        table_name_list[table_item.table_id] = table_item.name
              
    # print(table_name_list)
    # print(TABLE_SOURCE_LIST)
    

    # 遍历所有源多维表格中的数据表列表并获取其中的数据
    threadpool = []
    print("开始循环读取源数据表数据并写入到目标数据表")
    MESSAGE_LIST.append("开始循环读取源数据表数据并写入到目标数据表")
    for TABLE_SOURCE in TABLE_SOURCE_LIST:
          
      # print(TABLE_SOURCE)
      TABLE_SOURCE_NAME = table_name_list.get(TABLE_SOURCE)
      print("正在读取源数据表『" + TABLE_SOURCE_NAME + "』数据")
      MESSAGE_LIST.append("正在读取源数据表『" + TABLE_SOURCE_NAME + "』数据")

      has_more = True
      page_token = ""
      VIEW_ID = ""

      # 如果选择或输入的视图列表参数不为空，则通过视图名获取视图ID用于获取视图的记录
      if len(SELECT_VIEW) > 0:
        list_view_request = ListAppTableViewRequest.builder() \
          .page_size(500) \
          .table_id(TABLE_SOURCE) \
          .page_token(page_token) \
          .build()
        
        list_view_response = ""
        view_list = []
        try:
          list_view_response = client.base.v1.app_table_view.list(list_view_request)

        except Exception as e:
          time.sleep(2)
          list_view_response = client.base.v1.app_table_view.list(list_view_request)

        try:
          view_list = getattr(list_view_response.data, 'items', [])
        except Exception as e:
          pass
        # print(view_list)

        for view_item in view_list:
          if SELECT_VIEW[0] == view_item.view_name:
            VIEW_ID = view_item.view_id
            break
        # print(str(SELECT_VIEW[0]) + ": " + str(VIEW_ID))
          

      # 获取源数据表的字段列表
      INDEX_NUMBER = "Index Number"
      INDEX_NUMBER_FORMULA = "Index Number（Formula）"
      list_field_request = ListAppTableFieldRequest().builder() \
        .table_id(TABLE_SOURCE) \
        .build()
      
      list_field_response = client.base.v1.app_table_field.list(
            list_field_request)
      # print(list_field_response.data.__dict__)

      field_items = getattr(list_field_response.data, 'items', [])

      field_id = None
      index_field_id = None
      for field_item in field_items:
        if field_item.field_name == INDEX_NUMBER:
          field_id = field_item.field_id
        elif field_item.field_name == INDEX_NUMBER_FORMULA:
          index_field_id = field_item.field_id

      source_total = 0
      resp_code = ""
      if VIEW_ID == "":
        # 在源数据表中创建索引编号字段【Index Number】
        delete_field_response = ""

        if field_id is None or index_field_id is None:
          try:
            create_field_request = CreateAppTableFieldRequest().builder() \
              .table_id(TABLE_SOURCE) \
              .request_body(
                AppTableField.builder() \
                  .field_name(INDEX_NUMBER) \
                  .type(1005) \
                  .build()
              ) \
              .build()
            
            create_field_response = client.base.v1.app_table_field.create(
                  create_field_request)
            
            INDEX_NUMBER_FIELD_ID = create_field_response.data.field.field_id
            print("在源数据表创建【" + INDEX_NUMBER + "】字段")
            MESSAGE_LIST.append("在源数据表创建【" + INDEX_NUMBER + "】字段")

          except Exception as e:
            INDEX_NUMBER_FIELD_ID = field_id
          formula_expression = "bitable::$table["+TABLE_SOURCE+"].COUNTIF(CurrentValue.$column["+INDEX_NUMBER_FIELD_ID+"]<=bitable::$table["+TABLE_SOURCE+"].$field["+INDEX_NUMBER_FIELD_ID+"])"
          try:
            create_field_request = CreateAppTableFieldRequest().builder() \
              .table_id(TABLE_SOURCE) \
              .request_body(
                AppTableField.builder() \
                  .field_name(INDEX_NUMBER_FORMULA) \
                  .type(20) \
                  .property({"formula_expression": formula_expression}) \
                  .build()
              ) \
              .build()
            
            data_status = True
            while data_status:
              create_field_response = client.base.v1.app_table_field.create(
                    create_field_request)
              if create_field_response.code == 0:
                data_status = False
                
            if create_field_response.code == 1254014:
              raise "【" + INDEX_NUMBER_FORMULA + "】字段已存在"
            else:
              print("在源数据表创建【" + INDEX_NUMBER_FORMULA + "】字段")
              MESSAGE_LIST.append("在源数据表创建【" + INDEX_NUMBER_FORMULA + "】字段")

          except Exception as e:
            update_field_request = UpdateAppTableFieldRequest().builder() \
              .table_id(TABLE_SOURCE) \
              .field_id(index_field_id) \
              .request_body(
                AppTableField.builder() \
                  .field_name(INDEX_NUMBER_FORMULA) \
                  .type(20) \
                  .property({"formula_expression": formula_expression}) \
                  .build()
              ) \
              .build()
            
            update_field_response = client.base.v1.app_table_field.update(
                  update_field_request)
            print("在源数据表更新【" + INDEX_NUMBER_FORMULA + "】字段")
            MESSAGE_LIST.append("在源数据表更新【" + INDEX_NUMBER_FORMULA + "】字段")
            
        #   try:
        #     delete_field_request = DeleteAppTableFieldRequest().builder() \
        #       .app_token(APP_TOKEN) \
        #       .table_id(TABLE_SOURCE) \
        #       .field_id(field_id) \
        #       .build()
            
        #     delete_field_response = client.base.v1.app_table_field.delete(
        #           delete_field_request)

        #     print("在源数据表删除【" + INDEX_NUMBER + "】字段")
        #     MESSAGE_LIST.append("在源数据表删除【" + INDEX_NUMBER + "】字段")

        #   except Exception as e:
        #     print("在源数据表删除【" + INDEX_NUMBER + "】字段失败", e)
        #     pass

        # if delete_field_response == "" or delete_field_response.code == 0:
        #   try:
        #     create_field_request = CreateAppTableFieldRequest().builder() \
        #       .table_id(TABLE_SOURCE) \
        #       .request_body(
        #         AppTableField.builder() \
        #           .field_name(INDEX_NUMBER) \
        #           .type(1005) \
        #           .build()
        #       ) \
        #       .build()
            
        #     create_field_response = target_client.base.v1.app_table_field.create(
        #           create_field_request)
        #     # print(create_field_response.__dict__)
        #     print("在源数据表创建【" + INDEX_NUMBER + "】字段")
        #     MESSAGE_LIST.append("在源数据表创建【" + INDEX_NUMBER + "】字段")

        #   except Exception as e:
        #     print("在源数据表创建【" + INDEX_NUMBER + "】字段失败", e)
        #     pass

        FIELD_NAMES = [INDEX_NUMBER_FORMULA]
        search_records_resp = ""
        while resp_code != 0:
          search_records_resp = search_records(APP_TOKEN, PERSONAL_BASE_TOKEN, TABLE_SOURCE, '', page_token, None, FIELD_NAMES)
          if search_records_resp.get("code") == 0:
            source_total = search_records_resp.get("data").get("total")
            resp_code = 0

        # filter_info = {
        #                 "conjunction": "and",
        #                 "conditions": [
        #                   {
        #                     "field_name": INDEX_NUMBER,
        #                     "operator": "is",
        #                     "value": [source_total]
        #                   }
        #                 ]
        #               }
        
        # resp_code = ""
        # while resp_code != 0:
        #   search_records_resp = search_records(APP_TOKEN, PERSONAL_BASE_TOKEN, TABLE_SOURCE, '', page_token, filter_info, FIELD_NAMES)
        #   if search_records_resp.get("code") == 0:
        #     resp_code = 0

      else:
        while resp_code != 0:
          search_records_resp = search_records(APP_TOKEN, PERSONAL_BASE_TOKEN, TABLE_SOURCE, VIEW_ID, page_token, None, None)
          if search_records_resp.get("code") == 0:
            source_total = search_records_resp.get("data").get("total")
            resp_code = 0


      MESSAGE_LIST.append("源数据表『" + TABLE_SOURCE_NAME + "』共有 " + str(source_total) + " 条数据")
      print("源数据表『" + TABLE_SOURCE_NAME + "』共有 " + str(source_total) + " 条数据")
        
      
      # 获取视图记录
      count = 0
      record_list = []
      update_record_list = []
      records = []
      record_count = 0
      while has_more:
            
        ## 如果指定了记录ID，则仅从源表获取指定记录
        if RECORD_ID is not None and RECORD_ID != "":
          list_record_request = GetAppTableRecordRequest.builder() \
            .app_token(APP_TOKEN) \
            .table_id(TABLE_SOURCE) \
            .record_id(RECORD_ID) \
            .build()
          
          list_record_response = ""
          try:
            list_record_response = client.base.v1.app_table_record.get(
                  list_record_request)
          except Exception as e:
            time.sleep(2)
            list_record_response = client.base.v1.app_table_record.get(
                  list_record_request)
          # print(list_record_response.__dict__)
          if list_record_response.msg == 'RecordIdNotFound':
            return "指定的记录ID未找到，请检查后重试"
          else:
            records.append(list_record_response.data.record)

          has_more = False
          page_token = ""
          total = 1
            
        else:
              
          ## 如果没有选择视图，则按索引获取数据
          if VIEW_ID == "":
            step = 500
            filter_info_list = []
            future_list = []
            for i in range(0, source_total, step):
              last_num = min(i + step, source_total)
              # print(last_num)
              # print(source_total)
              print("正在读取 " + str(i + 1) + " ~ " + str(last_num) + " 行记录")
              filter_info = {
                              "conjunction": "and",
                              "conditions": [
                                {
                                  "field_name": INDEX_NUMBER_FORMULA,
                                  "operator": "isGreaterEqual",
                                  "value": [i + 1]
                                },
                                {
                                  "field_name": INDEX_NUMBER_FORMULA,
                                  "operator": "isLessEqual",
                                  "value": [last_num]
                                }
                              ]
                            }
              # print(filter_info)
              filter_info_list.append(filter_info)
              if len(filter_info_list) == 40 or last_num == source_total:
                # print(filter_info_list)

                with ThreadPoolExecutor(max_workers=4) as search_executor: 
                  for index in range(len(filter_info_list)):
                    future = search_executor.submit(search_records, APP_TOKEN, PERSONAL_BASE_TOKEN, TABLE_SOURCE, '', page_token, filter_info_list[index], None)
                    future_list.append(future)

                  for future_item in as_completed(future_list):
                    # print(future_item.result())
                    future_item.result()
                    try:
                      record_items = future_item.result().get("data").get("items")
                      records.extend(record_items)
                      # print(len(records))
                    except Exception as e:
                      continue
                
                filter_info_list = []
                future_list = []
              
              if last_num == source_total:
                has_more = False


          ## 如果选择了视图，则按视图和 page_token 获取数据
          else:
            search_records_resp = search_records(APP_TOKEN, PERSONAL_BASE_TOKEN, TABLE_SOURCE, VIEW_ID, page_token, None, None)
            # print(search_records_resp)

            if search_records_resp.get("code") == 0:
              has_more = search_records_resp.get("data").get("has_more")
              page_token = search_records_resp.get("data").get("page_token")
              total = search_records_resp.get("data").get("total")
              records = search_records_resp.get("data").get("items")

              # print(len(records))
              if not has_more:
                print("正在读取 " + str(record_count + 1) + " ~ " + str(total) + " 行记录")
                MESSAGE_LIST.append("正在读取 " + str(record_count + 1) + " ~ " + str(total) + " 行记录")
              else:
                print("正在读取 " + str(record_count + 1) + " ~ " + str(record_count + 500) + " 行记录")
                MESSAGE_LIST.append("正在读取 " + str(record_count + 1) + " ~ " + str(record_count + 500) + " 行记录")

          # print(len(records))
          # print("=" * 30)

          fields = {}
          if records is None:
            records = []
          for record_item in records:
            record_count += 1
            fields = record_item.get("fields")
            field_list = {}
            source_index_field_value = ""
            for key, value in fields.items():

              # print("=" * 50)
              # print(str(key) + ": " + str(value))
              # print("=" * 50)

              # 获取当前行索引字段的值
              if key == INDEX_FIELD_NAME:
                try:
                  source_index_field_value = value[0].get("text")
                except Exception as e:
                  source_index_field_value = value

              if key in FIELD_TARGET:
                # 【文本】和【条码】字段，支持【自动编号】写入
                if FIELD_TARGET_META[key] == 1 or FIELD_TARGET_META[key] == 99001:
                  if isinstance(value, (list)):
                    value_tmp = ""
                    if len(value) == 1:
                      try:
                        value = value[0].get("text")
                      except Exception as e:
                        value = str(value)
                    else:
                      for item in value:
                        try:
                          value_tmp = value_tmp + item.get("link")
                        except Exception as e:
                          value_tmp = value_tmp + item.get("text")
                      value = value_tmp
                  elif isinstance(value, (dict)):
                    try:
                      value = value.get("value")[0].get("text")
                    except Exception as e:
                      value = str(value)
                  elif isinstance(value, (int, float)):
                    value = str(value)
                  else:
                    value = str(value)

                # 【数字】、【进度】、【货币】和【评分】字段，支持【自动编号】数字格式写入
                elif FIELD_TARGET_META[key] == 2 or FIELD_TARGET_META[key] == 99002 or FIELD_TARGET_META[key] == 99003 or FIELD_TARGET_META[key] == 99004:
                  if isinstance(value, (list)):
                    if len(value) > 0:
                      value = value[0]
                    else:
                      value = None
                  elif isinstance(value, (dict)):
                    try:
                      value = value.get("value")[0]
                    except Exception as e:
                      value = None
                  elif isinstance(value, (str)):
                    try:
                      value = int(value)
                    except Exception as e:
                      try:
                        value = float(value)
                      except Exception as e:
                        value = None
                  elif isinstance(value, (int, float)):
                    pass
                  else:
                    value = None
                  
                  if value is not None:
                    try:
                      if float(value) % 1 == 0:
                        value = int(value)
                      else:
                        value = float(value)
                    except Exception as e:
                      value = None
                
                # 【单选】字段，支持【文本】和【数字】字段写入
                elif FIELD_TARGET_META[key] == 3:
                  if isinstance(value, (dict)):
                    try:
                      value = str(value.get("value")[0])
                    except Exception as e:
                      value = None
                  elif isinstance(value, (int, float)):
                    value = str(value)
                  elif isinstance(value, (str)):
                    pass
                  else:
                    value = None

                # 【多选】字段
                elif FIELD_TARGET_META[key] == 4:
                  if isinstance(value, (dict)):
                    try:
                      value = value.get("value")
                    except Exception as e:
                      value = None
                  elif isinstance(value, (list)):
                    value_tmp = value[0]
                    if not isinstance(value_tmp, (str)):
                      value = None
                  else:
                    value = None

                  if value is not None:
                    if not isinstance(value, (list)):
                      value = None

                # 【日期】字段，支持【创建日期】和【最后更新日期】字段写入
                elif FIELD_TARGET_META[key] == 5:
                  if isinstance(value, (dict)):
                    try:
                      value = value.get("value")[0]
                    except Exception as e:
                      value = None
                  elif isinstance(value, (list)):
                    try:
                      value = value[0].get("text")
                    except Exception as e:
                      value = value[0]
                  elif isinstance(value, (int, float)):
                    pass
                  else:
                    value = None
                  
                  if value is not None:
                    if isinstance(value, (str)):
                      try:
                        value = int(value)
                      except Exception as e:
                        value = None
                    elif isinstance(value, (int, float)):
                      pass
                    else:
                      value = None

                    if len(str(value)) != 13:
                      value = None
                  
                # 【复选框】字段，支持【文本】字段和【数字】字段中保存的布尔值写入
                elif FIELD_TARGET_META[key] == 7:
                  if isinstance(value, (dict)):
                    try:
                      value = value.get("value")[0].get("text")
                      if value in ["true", "True", "TRUE", "1"]:
                        value = True
                      elif value in ["false", "False", "FALSE", "0"]:
                        value = False
                      else:
                        value = None
                    except Exception as e:
                      try:
                        value = value.get("value")[0]
                        if value == 1:
                          value = True
                        elif value == 0:
                          value = False
                        else:
                          value = None
                      except Exception as e:
                        value = None
                  elif isinstance(value, (list)):
                    value = value[0]
                    if value == 1 or value in ["true", "True", "TRUE", "1"]:
                      value = True
                    elif value == 0 or value in ["false", "False", "FALSE", "0"]:
                      value = False
                    else:
                      value = None
                  elif isinstance(value, (int, float)):
                    if value == 1:
                      value = True
                    elif value == 0:
                      value = False
                    else:
                      value = None
                  elif isinstance(value, (bool)):
                    pass
                  else:
                    value = None
                
                # 【人员】字段，支持【创建人】和【修改人】字段写入
                elif FIELD_TARGET_META[key] == 11:
                  if isinstance(value, dict):
                    value = value.get("value", None)
                  elif isinstance(value, list):
                    pass
                  else:
                    value = None

                  if value is not None:
                    try:
                      value_tmp = value[0].get("id")
                    except Exception as e:
                      value = None

                # 【电话号码】字段
                elif FIELD_TARGET_META[key] == 13:
                  if isinstance(value, dict):
                    try:
                      value = value.get("value")[0]
                    except Exception as e:
                      value = value.get("value", None)
                  elif isinstance(value, list):
                    value = str(value[0])
                  elif isinstance(value, str):
                    pass
                  elif isinstance(value, (int, float)):
                    value = str(value)
                  else:
                    value = None

                  if value is not None:
                    pattern = r'^\+?\d+$'
                    if re.match(pattern, value) is None:
                      value = None

                # 【超链接】字段
                elif FIELD_TARGET_META[key] == 15:
                  if isinstance(value, dict):
                    try:
                      value = value.get("value")[0]
                    except Exception as e:
                      pass
                  else:
                    value = None

                  if value is not None:
                    try:
                      value_tmp = value.get("link")
                    except Exception as e:
                      value = None

                # 【附件】字段
                elif FIELD_TARGET_META[key] == 17:
                  if isinstance(value, dict):
                    try:
                      value = value.get("value")
                    except Exception as e:
                      pass
                  elif isinstance(value, list):
                    pass
                  else:
                    value = None
                    
                  if value is not None:
                    try:
                      value_tmp = value[0].get("file_token")
                    except Exception as e:
                      value = None

                # 【单向关联】和【双向关联】字段
                elif FIELD_TARGET_META[key] == 18 or FIELD_TARGET_META[key] == 21:
                  if isinstance(value, dict) and value != {}:
                    try:
                      value = value[0].get("record_ids")
                    except Exception as e:
                      try:
                        value = value.get("link_record_ids")
                      except Exception as e:
                        value = None
                  else:
                    value = None

                  if len(FATHER_FIELD_TARGET) > 0 and FATHER_FIELD_TARGET in FIELD_TARGET:
                    if FATHER_FIELD_TARGET == key and value is not None:
                      value = value[0]
                      key = SOURCE_PARENT_FIELD

                # 【地理位置】字段，支持【文本】字段中保存经纬度坐标写入
                elif FIELD_TARGET_META[key] == 22:
                  if isinstance(value, dict):
                    try:
                      value = value.get("value")[0].get("location")
                    except Exception as e:
                      value = value.get("location", None)
                  elif isinstance(value, list):
                    value = value[0].get("text", None)
                  else:
                    value = None

                  if value is not None:
                    pattern = r'^\d+(\.\d+)?,\d+(\.\d+)?$'
                    if re.match(pattern, value) is None:
                      value = None

                # 【群组】字段
                elif FIELD_TARGET_META[key] == 23:
                  if isinstance(value, dict):
                    try:
                      value = value.get("value")
                    except Exception as e:
                      pass
                  elif isinstance(value, list):
                    pass
                  else:
                    value = None
                  
                  if value is not None:
                    try:
                      value_tmp = value[0].get("avatar_url")
                    except Exception as e:
                      value = None

                # 其他情况均为 None
                else:
                  value = None
                
                field_list[key] = value
                try:
                  field_list[SOURCE_RECORD_ID] = record_item.record_id
                except Exception as e:
                  field_list[SOURCE_RECORD_ID] = record_item.get("record_id")

                field_list[SOURCE_TABLE] = table_name_list.get(TABLE_SOURCE)
              # print(field_list)


            # 判断目标数据表中的记录是否为空
            if target_record_id_map != {}:
              
              if INDEX_FIELD_NAME == 'Source Record ID':
                target_record_id = target_record_id_map.get(record_item.get("record_id"), None)
              else:
                target_record_id = target_record_id_map.get(source_index_field_value, None)
              
              # 若源数据表记录存在于目标数据表中则执行更新合并
              if target_record_id is not None:
                
                # 判断是否写入预设值
                if DEFAULTVALUE_MAPPING_MODE == 'both' or DEFAULTVALUE_MAPPING_MODE == 'update':
                  for default_key, default_value in DEFAULTVALUE_MAPPING_VALUE.items():
                    # print(str(default_key) + ": " + str(default_value))
                    # print("=" * 50)
                    try:
                      get_field_info = field_list[default_key]
                    except Exception as e:
                      if default_key in FIELD_TARGET:
                        if is_test(str(default_value)):
                          source_key = default_value.replace("{{", "").replace("}}", "")
                          ref_field_value = fields.get(source_key, None)

                          if FIELD_TARGET_META[default_key] == 5:
                            try:
                              try:
                                ref_field_value = ref_field_value[0].get("text", None)
                                ref_field_value = re.sub('[/年月]', '-', ref_field_value)
                                ref_field_value = re.sub('[日]', ' ', ref_field_value)
                                ref_field_value = re.sub('[时分]', ':', ref_field_value)
                                ref_field_value = re.sub('[秒]', '', ref_field_value)
                                try:
                                  time_tuple = time.strptime(ref_field_value, "%Y-%m-%d %H:%M:%S")
                                except Exception as e:
                                  time_tuple = time.strptime(ref_field_value, "%Y-%m-%d")
                              except Exception as e:
                                try:
                                  time_tuple = time.strptime(ref_field_value, "%Y-%m-%d %H:%M:%S")
                                except Exception as e:
                                  time_tuple = time.strptime(ref_field_value, "%Y-%m-%d")
                              ref_field_value = int(time.mktime(time_tuple)) * 1000

                            except Exception as e:
                              if len(str(ref_field_value)) == 13:
                                pass
                              elif len(str(ref_field_value)) == 10:
                                ref_field_value = ref_field_value * 1000
                              else:
                                ref_field_value = None

                          elif FIELD_TARGET_META[default_key] == 11:
                            try:
                              ref_field_value = ref_field_value.get("users", None)
                            except Exception as e:
                              pass

                          field_list[default_key] = ref_field_value
                        else:
                          field_list[default_key] = default_value

                # 判断更新策略为clear或retain，若为clear则清空目标表中的其余字段数据，默认为retain模式
                if DEFAULTVALUE_UPDATING_POLICY == 'clear':
                  for field_item in FIELD_TARGET:
                    try:
                      get_field_info = field_list[field_item]
                    except Exception as e:
                      field_list[field_item] = None

                update_record_list.append({"record_id": target_record_id, "fields": field_list})
                total_update = total_update + 1

              # 若源数据表记录不存在于目标数据表中则执行新增合并
              else:
                # 判断是否写入预设值
                if DEFAULTVALUE_MAPPING_MODE == 'both' or DEFAULTVALUE_MAPPING_MODE == 'new':
                  for default_key, default_value in DEFAULTVALUE_MAPPING_VALUE.items():
                    # print(str(default_key) + ": " + str(default_value))
                    try:
                      get_field_info = field_list[default_key]
                    except Exception as e:
                      if default_key in FIELD_TARGET:
                        if is_test(str(default_value)):
                          source_key = default_value.replace("{{", "").replace("}}", "")
                          ref_field_value = fields.get(source_key, None)

                          if FIELD_TARGET_META[default_key] == 5:
                            try:
                              try:
                                ref_field_value = ref_field_value[0].get("text", None)
                                ref_field_value = re.sub('[/年月]', '-', ref_field_value)
                                ref_field_value = re.sub('[日]', ' ', ref_field_value)
                                ref_field_value = re.sub('[时分]', ':', ref_field_value)
                                ref_field_value = re.sub('[秒]', '', ref_field_value)
                                try:
                                  time_tuple = time.strptime(ref_field_value, "%Y-%m-%d %H:%M:%S")
                                except Exception as e:
                                  time_tuple = time.strptime(ref_field_value, "%Y-%m-%d")
                              except Exception as e:
                                try:
                                  time_tuple = time.strptime(ref_field_value, "%Y-%m-%d %H:%M:%S")
                                except Exception as e:
                                  time_tuple = time.strptime(ref_field_value, "%Y-%m-%d")
                              ref_field_value = int(time.mktime(time_tuple)) * 1000

                            except Exception as e:
                              if len(str(ref_field_value)) == 13:
                                pass
                              elif len(str(ref_field_value)) == 10:
                                ref_field_value = ref_field_value * 1000
                              else:
                                ref_field_value = None

                          elif FIELD_TARGET_META[default_key] == 11:
                            try:
                              ref_field_value = ref_field_value.get("users", None)
                            except Exception as e:
                              pass

                          field_list[default_key] = ref_field_value
                        else:
                          field_list[default_key] = default_value
                
                if field_list != {}:
                  record_list.append({"fields": field_list})
                  total_create = total_create + 1
                else:
                  total = total - 1

            # 若目标数据表中的记录为空则执行新增合并
            else:
              # 判断是否写入预设值
              if DEFAULTVALUE_MAPPING_MODE == 'both' or DEFAULTVALUE_MAPPING_MODE == 'new':
                for default_key, default_value in DEFAULTVALUE_MAPPING_VALUE.items():
                  # print(str(default_key) + ": " + str(default_value))
                  try:
                    get_field_info = field_list[default_key]
                  except Exception as e:
                    if default_key in FIELD_TARGET:
                      if is_test(str(default_value)):
                        source_key = default_value.replace("{{", "").replace("}}", "")
                        ref_field_value = fields.get(source_key, None)
                        
                        if FIELD_TARGET_META[default_key] == 5:
                          try:
                            try:
                              ref_field_value = ref_field_value[0].get("text", None)
                              ref_field_value = re.sub('[/年月]', '-', ref_field_value)
                              ref_field_value = re.sub('[日]', ' ', ref_field_value)
                              ref_field_value = re.sub('[时分]', ':', ref_field_value)
                              ref_field_value = re.sub('[秒]', '', ref_field_value)
                              try:
                                time_tuple = time.strptime(ref_field_value, "%Y-%m-%d %H:%M:%S")
                              except Exception as e:
                                time_tuple = time.strptime(ref_field_value, "%Y-%m-%d")
                            except Exception as e:
                              try:
                                time_tuple = time.strptime(ref_field_value, "%Y-%m-%d %H:%M:%S")
                              except Exception as e:
                                time_tuple = time.strptime(ref_field_value, "%Y-%m-%d")
                            ref_field_value = int(time.mktime(time_tuple)) * 1000

                          except Exception as e:
                            if len(str(ref_field_value)) == 13:
                              pass
                            elif len(str(ref_field_value)) == 10:
                              ref_field_value = ref_field_value * 1000
                            else:
                              ref_field_value = None

                        elif FIELD_TARGET_META[default_key] == 11:
                          try:
                            ref_field_value = ref_field_value.get("users", None)
                          except Exception as e:
                            pass

                        field_list[default_key] = ref_field_value
                      else:
                        field_list[default_key] = default_value

              if field_list != {}:
                record_list.append({"fields": field_list})
                total_create = total_create + 1
              else:
                total = total - 1
            
            count += 1

            # print(TABLE_SOURCE, str(count))
            # print(len(record_list))
            if len(record_list) == 3000 or record_count == source_total:
              if len(record_list) > 0:
                step = 1000
                for i in range(0, len(record_list), step):
                  new_record_id_list = record_list[i:i + step]
                  future = executor.submit(batch_create_record_func, TARGET_APP_TOKEN, TARGET_PERSONAL_BASE_TOKEN, TABLE_TARGET, new_record_id_list)
                  threadpool.append(future)
                record_list = []

            if len(update_record_list) == 3000 or record_count == source_total:
              if len(update_record_list) > 0:
                step = 1000
                for i in range(0, len(update_record_list), step):
                  new_record_id_list = update_record_list[i:i + step]
                  future = executor.submit(batch_update_record_func, TARGET_APP_TOKEN, TARGET_PERSONAL_BASE_TOKEN, TABLE_TARGET, new_record_id_list)
                  threadpool.append(future)
                update_record_list = []

            wait(threadpool, return_when=ALL_COMPLETED)

  executor.shutdown(wait=True)

  result = "总共合并 " + str(total_create + total_update) + " 条记录（新增 " + str(total_create) + " 条，更新 " + str(total_update) + " 条）"
  print(result)
  MESSAGE_LIST.append(result)

  # 如果父记录字段不为空，则执行父子记录关系的自动匹配
  if len(FATHER_FIELD_TARGET) > 0:
    print("正在自动关联父子记录层级关系")
    MESSAGE_LIST.append("正在自动关联父子记录层级关系")
    has_more = True
    page_token = ""

    filter_info = "AND(CurrentValue.["+TARGET_PARENT_RECORD_ID+"]!=\"\",CurrentValue.["+FATHER_FIELD_TARGET+"]=\"\")"
    while has_more:
      filter_info = {
                      "conjunction": "and",
                      "conditions": [
                        {
                          "field_name": TARGET_PARENT_RECORD_ID,
                          "operator": "isNotEmpty",
                          "value": []
                        },
                        {
                          "field_name": FATHER_FIELD_TARGET,
                          "operator": "isEmpty",
                          "value": []
                        }
                      ]
                    }

      # 调用接口查询数据
      search_records_resp = search_records(TARGET_APP_TOKEN, TARGET_PERSONAL_BASE_TOKEN, TABLE_TARGET, '', page_token, filter_info, None)
      
      has_more = search_records_resp.get("data").get("has_more")
      page_token = search_records_resp.get("data").get("page_token")
      total = search_records_resp.get("data").get("total")
      records = search_records_resp.get("data").get("items")
      # print(records)
      # print(len(records))

      if total > 0:
        if records is None:
          records = []
        for record_item in records:
          FIELD_INDEX_VALUE = record_item.get("fields").get(INDEX_FIELD_NAME, None)
          if FIELD_INDEX_VALUE is not None:
            FIELD_INDEX_VALUE = FIELD_INDEX_VALUE[0].get("text")
            target_record_id_map[FIELD_INDEX_VALUE] = record_item.get("record_id")

      update_record_list = []
      record_id = ""
      if records is None:
        records = []
      for record_item in records:
        try:
          fields = record_item.fields
          record_id = record_item.record_id
        except Exception as e:
          fields = record_item.get("fields")
          record_id = record_item.get("record_id")
        field_list = {}
        source_index_field_value = ""
        for key, value in fields.items():
          if key == TARGET_PARENT_RECORD_ID:
            try:
              value = [value[0].get("text")]
            except Exception as e:
              value = [value.get("value")[0].get("text")]
            field_list[FATHER_FIELD_TARGET] = value
            break
        update_record_list.append({"record_id": record_id, "fields": field_list})

    # print(update_record_list)
    # print(len(update_record_list))
    if len(update_record_list) > 0:
      result_response = batch_update_record_func(TARGET_APP_TOKEN, TARGET_PERSONAL_BASE_TOKEN, TABLE_TARGET, update_record_list)
      # time.sleep(2)

      if result_response == '记录更新成功':
        result = "共有 " + str(len(update_record_list)) + " 条子记录自动关联完成"
        MESSAGE_LIST.append(result)
      else:
        result = "自动关联失败，请人工进行关联"
        MESSAGE_LIST.append(result)
    else:
      result = "本次无记录需要自动关联"
      MESSAGE_LIST.append(result)
          
  # 当前任务结束时间
  end_timestamp = time.time() 
  local_end_time = time.localtime()    
  formatted_local_end_time = time.strftime('%Y-%m-%d %H:%M:%S', local_end_time)
  time_format = "结束时间：" + formatted_local_end_time
  print(time_format)
  MESSAGE_LIST.append(time_format)
  if end_timestamp - start_timestamp >= 60:
    time_format = "执行时长：" + str(math.floor((end_timestamp - start_timestamp)/60)) + " 分 " + str(math.floor((end_timestamp - start_timestamp) % 60)) + " 秒"
    print(time_format)
    MESSAGE_LIST.append(time_format)
  else:
    time_format = "执行时长：" + str(math.floor(end_timestamp - start_timestamp)) + " 秒"
    print(time_format)
    MESSAGE_LIST.append(time_format)

  MESSAGE_LIST.append("======================================")

  # print(MESSAGE_LIST)
  result = "\r\n".join(MESSAGE_LIST)

  # print(1, result, flush=True)
  print(result)
  return result



###########################   批量创建记录，数据列表拆分成按照每1000条记录写入一次   ###########################
def batch_create_record_func(APP_TOKEN: str, PERSONAL_BASE_TOKEN : str, TABLE_ID: str, record_list: object):
     
  global MESSAGE_LIST

  # print(record_list)
  try:

    # 1. 构建client
    client: BaseClient = BaseClient.builder() \
        .app_token(APP_TOKEN) \
        .personal_base_token(PERSONAL_BASE_TOKEN) \
        .build()
    
    step = 1000
    for i in range(0, len(record_list), step):
      new_record_list = record_list[i:i + step]
      retry = 0
      while retry < 5:
        try:
          # 2. 批量保存单次的数据到表
          batch_create_records_request = BatchCreateAppTableRecordRequest().builder() \
          .table_id(TABLE_ID) \
          .request_body(
            BatchCreateAppTableRecordRequestBody.builder() \
              .records(new_record_list) \
              .build()
          ) \
          .build()

          batch_create_records_response = client.base.v1.app_table_record.batch_create(
              batch_create_records_request)
          # print(batch_create_records_response.__dict__)

          if batch_create_records_response.code == 0:
            retry = 5
            result = "记录创建成功"
          else:
            print("记录创建失败")
            MESSAGE_LIST.append("记录创建失败")
            raise "记录创建失败"

        except Exception as e:
          if batch_create_records_response.code == 1254103:
            result = "目标数据表最大记录行数超限，请升级后再进行合并"
            MESSAGE_LIST.append(result)
            break

          retry = retry + 1
          if retry == 5:
            result = "写入数据尝试超过5次，数据操作失败，请检查网络！"
            MESSAGE_LIST.append(result)
          else:
            retry_time = retry * 2
            time.sleep(retry_time)
            print("正在尝试第 " + str(retry) + " 次创建记录")
            MESSAGE_LIST.append("正在尝试第 " + str(retry) + " 次创建记录")
            
    if batch_create_records_response.code != 0 and batch_create_records_response.code != 1254103:
      result = "错误信息：" + batch_create_records_response.msg + " 错误代码：" + str(batch_create_records_response.code)
      MESSAGE_LIST.append(result)

  except Exception as e:
    result = "记录创建失败"
    print(result, e)
    MESSAGE_LIST.append(str(e))
    MESSAGE_LIST.append(result)
  
  # print(result)
  return result


###########################   批量更新记录，数据列表拆分成按照每1000条记录更新一次   ###########################
def batch_update_record_func(APP_TOKEN: str, PERSONAL_BASE_TOKEN : str, TABLE_ID: str, record_list: object):
   
  global MESSAGE_LIST

  # print(record_list)
  # print(len(record_list))
  try:

    # 1. 构建client
    client: BaseClient = BaseClient.builder() \
        .app_token(APP_TOKEN) \
        .personal_base_token(PERSONAL_BASE_TOKEN) \
        .build()
    
    step = 1000
    for i in range(0, len(record_list), step):
      new_record_list = record_list[i:i + step]
      retry = 0
      while retry < 5:
        try:
          # 2. 批量更新单次的数据到表
          batch_update_records_request = BatchUpdateAppTableRecordRequest().builder() \
          .table_id(TABLE_ID) \
          .request_body(
            BatchUpdateAppTableRecordRequestBody.builder() \
              .records(new_record_list) \
              .build()
          ) \
          .build()

          batch_update_records_response = client.base.v1.app_table_record.batch_update(
              batch_update_records_request)
          # print(batch_update_records_response.__dict__)

          if batch_update_records_response.code == 0:
            retry = 5
            result = "记录更新成功"
          else:
            print("记录更新失败")
            MESSAGE_LIST.append("记录更新失败")
            raise "记录更新失败"

        except Exception as e:
          retry = retry + 1
          if retry == 5:
            result = "更新数据尝试超过5次，数据更新失败，请检查网络！"
            MESSAGE_LIST.append(result)
          else:
            retry_time = retry * 2
            time.sleep(retry_time)
            print("正在尝试第 " + str(retry) + " 次更新记录")
            MESSAGE_LIST.append("正在尝试第 " + str(retry) + " 次更新记录")
          
    if batch_update_records_response.code != 0:
      result = "错误信息：" + batch_update_records_response.msg + " 错误代码：" + str(batch_update_records_response.code)
      MESSAGE_LIST.append(result)

  except Exception as e:
    result = "记录更新失败"
    print(result, e)
    MESSAGE_LIST.append(str(e))
    MESSAGE_LIST.append(result)

  # print(result)
  return result
  


###########################   批量删除数据，数据列表拆分成按照每500条记录删除一次   ###########################
def batch_delete_record_func(APP_TOKEN: str, PERSONAL_BASE_TOKEN : str, TABLE_ID: str, record_id_list: object):
  
  # print(record_id_list)
  try:
    # 1. 构建client
    client: BaseClient = BaseClient.builder() \
        .app_token(APP_TOKEN) \
        .personal_base_token(PERSONAL_BASE_TOKEN) \
        .build()
    
    step = 500
    for i in range(0, len(record_id_list), step):
      new_record_id_list = record_id_list[i:i + step]

      retry = 0
      while retry < 5:
        try:
          # 2. 删除视图中的全部数据
          batch_delete_records_request = BatchDeleteAppTableRecordRequest().builder() \
              .app_token(APP_TOKEN) \
              .table_id(TABLE_ID) \
              .request_body(
                BatchDeleteAppTableRecordRequestBody.builder() \
                  .records(new_record_id_list) \
                  .build()
              ) \
              .build()

          batch_delete_records_response = client.base.v1.app_table_record.batch_delete(
              batch_delete_records_request)
          # print(batch_delete_records_response.__dict__)
          
          if batch_delete_records_response.code == 0:
            retry = 5
            result = "记录删除成功"
          else:
            print("记录删除失败")
            raise "记录删除失败"

        except Exception as e:
          retry = retry + 1
          if retry == 5:
            result = "删除数据尝试超过5次，数据删除失败，请检查网络！"
          else:
            retry_time = retry * 2
            time.sleep(retry_time)
            print("正在尝试第 " + str(retry) + " 次删除记录")

    if batch_delete_records_response.code != 0:
      result = "错误信息：" + batch_delete_records_response.msg + " 错误代码：" + str(batch_delete_records_response.code)
  
  except Exception as e:
    result = "记录删除失败"
    print(result, e)
  
  # print(result)
  return result
  
  

###########################   调用 base 开放接口批量删除记录，解决大容量数据表批量删除数据问题   ###########################
def batch_delete_records(APP_TOKEN: str, PERSONAL_BASE_TOKEN : str, TABLE_ID: str, record_id_list: list):
 
  global MESSAGE_LIST

  # 批量删除记录接口
  BATCH_DELETE_RECORDS_URL = 'https://base-api.feishu.cn/open-apis/bitable/v1/apps/' + APP_TOKEN + '/tables/' + TABLE_ID + '/records/batch_delete'

  batch_delete_records_headers = {
    'Content-Type': 'application/json; charset=utf-8',
    'Authorization': 'Bearer ' + PERSONAL_BASE_TOKEN
  }

  step = 500
  for i in range(0, len(record_id_list), step):
    new_record_id_list = record_id_list[i:i + step]

    batch_delete_records_req_body = {
      "records": new_record_id_list
    }

    batch_delete_records_resp = ""
    retry = 0
    while retry < 5:
      try:
        batch_delete_records_resp = requests.post(url=BATCH_DELETE_RECORDS_URL, headers=batch_delete_records_headers, json=batch_delete_records_req_body)

        if batch_delete_records_resp.status_code == 200:
          retry = 5
          result = "记录删除成功"
        elif batch_delete_records_resp.status_code == 429:
          print("请求太频繁，等待重试")
          MESSAGE_LIST.append("请求太频繁，等待重试")
          raise "请求太频繁，等待重试"
        else:
          print("记录删除失败")
          MESSAGE_LIST("记录删除失败")
          raise "记录删除失败"
      except Exception as e:
        retry = retry + 1
        if retry == 5:
          print("删除记录超过 5 次")
          result = "删除记录超过 5 次"
          MESSAGE_LIST(result)
        else:
          retry_time = retry * 2
          time.sleep(retry_time)
          print("正在尝试第 " + str(retry) + " 次删除记录")
          MESSAGE_LIST.append("正在尝试第 " + str(retry) + " 次删除记录")
  
  # print(result)
  return result



###########################   调用 base 开放接口查询记录，解决大容量数据表读取数据问题   ###########################
def search_records(APP_TOKEN: str, PERSONAL_BASE_TOKEN : str, TABLE_ID: str, VIEW_ID: str|None, PAGE_TOKEN: str, FILTER_INFO: dict|None, FIELD_NAMES: list|None):

  global MESSAGE_LIST

  # 查询记录接口
  SEARCH_RECORDS_URL = 'https://base-api.feishu.cn/open-apis/bitable/v1/apps/' + APP_TOKEN + '/tables/' + TABLE_ID + '/records/search'

  search_records_headers = {
    'Content-Type': 'application/json; charset=utf-8',
    'Authorization': 'Bearer ' + PERSONAL_BASE_TOKEN
  }

  search_records_queries = {
    'page_token': PAGE_TOKEN,
    'page_size': 500
  }

  search_records_req_body = {}
  if VIEW_ID != "" or VIEW_ID is not None:
    search_records_req_body['view_id'] = VIEW_ID

  if FILTER_INFO is not None:
    search_records_req_body['filter'] = FILTER_INFO

  if FIELD_NAMES is not None:
    search_records_req_body['field_names'] = FIELD_NAMES

  search_records_resp = ""
  retry = 0
  while retry < 5:
    try:
      search_records_resp = requests.post(url=SEARCH_RECORDS_URL, params=search_records_queries, headers=search_records_headers, json=search_records_req_body)
      # print(search_records_resp.json())
      if search_records_resp.status_code == 200:
        retry = 5
        search_records_resp = search_records_resp.json()
        # print(search_records_resp)
      elif search_records_resp.status_code == 429:
        print("请求太频繁，等待重试")
        MESSAGE_LIST.append("请求太频繁，等待重试")
        raise "请求太频繁，等待重试"
    except Exception as e:
      retry = retry + 1
      if retry == 5:
        print("获取数据超过 5 次")
        MESSAGE_LIST.append("获取数据超过 5 次")
        search_records_resp = {}
      else:
        retry_time = retry * 2
        time.sleep(retry_time)
        print("正在尝试第 " + str(retry) + " 次查询记录")
        MESSAGE_LIST.append("正在尝试第 " + str(retry) + " 次查询记录")
  
  # print(search_records_resp.status_code)
  return search_records_resp
