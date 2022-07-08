import datetime
import os
import re
import time
from decimal import Decimal
import requests
import eventlet
import urllib
import hmac
import platform
import base64
from hashlib import sha256
from .models import CompanyBear, PayBehalf
import xlrd
from dateutil.relativedelta import relativedelta

from application import settings
from apps.ehr.models import Department, Em_Basic
from apps.salary_conf.models import Formula, Zero
from apps.salary.filters import (
    SalaryDesignFilter,
    SummaryDesignFilter,
    SalaryAdminFilter,
    SummaryAdminFilter,
)
from apps.salary.models import (
    SalaryDesign,
    SummaryDesign,
    SyncAccessToken,
    SalaryAdmin,
    SummaryAdmin,
    SalaryFangDiChan,
    SummaryFangDiChan,
    SalaryShanDongDade,
    SummaryShanDongDade,
    SalaryJinanPuSheng,
    SummaryJinanPuSheng,
)
from apps.salary.serializers import (
    SalaryDesignSerializer,
    SalaryDesignCreateUpdateSerializer,
    ExportSalaryDesignSerializer,
    ImportSalaryDesignSerializer,
    ExportSummaryDesignSerializer,
    SummaryDesignCreateUpdateSerializer,
    SummaryDesignSerializer,
    SalaryAdminCreateUpdateSerializer,
    SummaryAdminCreateUpdateSerializer,
    SummaryAdminSerializer,
    SalaryAdminSerializer,
    ImportSalaryAdminSerializer,
    ExportSummaryAdminSerializer,
    ExportSalaryAdminSerializer,
)
from apps.vadmin.op_drf.filters import DataLevelPermissionsFilter
from apps.vadmin.permission.permissions import CommonPermission
from apps.vadmin.op_drf.viewsets import CustomModelViewSet
import json
from rest_framework.request import Request
from apps.vadmin.op_drf.response import SuccessResponse, ErrorResponse
from apps.vadmin.utils.export_excel import excel_to_data, export_excel_save_model
from apps.vadmin.utils.request_util import get_verbose_name
from django.db.models import Sum, Q


class CustomModelViewSetBase(CustomModelViewSet):
    """base"""

    url = "https://api.diwork.com/yonbip/fi/ficloud/openapi/voucher/addVoucher"
    dept_code = {
        "分公司": "D",
        "临沂分公司": "D01",
        "枣庄分公司": "D02",
        "安徽分公司": "D03",
        "烟台分公司": "D04",
        "青岛分公司": "D05",
        "烟台分公司（新）": "D06",
        "临时1": "L",
        "管理部门": "M",
        "总工办": "M01",
        "运营部": "M02",
        "市场部": "M03",
        "综合部": "M04",
        "财务部": "M05",
        "信息管理中心": "M06",
        "人力资源中心": "M07",
        "行政管理中心": "M08",
        "院领导": "M09",
        "业务部门": "P",
        "一分院": "P01",
        "二分院": "P02",
        "三分院": "P03",
        "四分院": "P04",
        "五分院": "P05",
        "六分院": "P06",
        "机电分院": "P07",
        "医疗建筑设计分院": "P08",
        "创研一室": "P09",
        "创研二室": "P10",
        "张冰工作室": "P11",
        "数字技术中心": "P12",
        "消防专委会": "P13",
        "七分院": "P14",
        "八分院": "P15",
        "九分院": "P16",
        "十分院": "P17",
        "十一分院": "P18",
        "人防分院": "P19",
        "装饰一分院": "P20",
        "装饰二分院": "P21",
        "医疗装饰分院": "P22",
        "景观分院": "P23",
        "加固分院": "P24",
        "幕墙分院": "P25",
        "智能化分院": "P26",
        "市政分院": "P27",
        "造价分院": "P28",
        "机械分院": "P29",
        "工程管理咨询分院": "P30",
        "全过程项目管理中心": "P31",
        "临时2": "S",
        # "综合部": "001",
        "设计部": "002",
    }
    sync_voucher = {
        "ff": {  # 发放
            "key": "af73f1abe3b34d418df1a8ca3faeadf5",
            "secret": "31122bfb85f04d319311570e000a39b9",
            "mobile": "17686618902",
        },
        "fp": {  # 分配
            "key": "87f7a4710bef4b0c884a759c21864f4c",
            "secret": "e3e31eb478c24714b196d509b49b1385",
            "mobile": "17686610667",
        },
    }

    def conversion_time(self, time: datetime):
        return "{}-{}".format(
            str(time.year),
            str(time.month) if len(str(time.month)) != 1 else "0" + str(time.month),
        )

    def check_contain_chinese(self, check_str):
        for ch in check_str:
            if "\u4e00" <= ch <= "\u9fff":
                return True
        return False

    def compute(self, type, request, *args, **kwargs):
        print(f"----compute data---type={type}-------")
        queryset_f = Formula.objects
        if type == "design":
            base_query = SalaryDesign
            queryset_f = queryset_f.filter(type="设计人员")
        elif type == "admin":
            base_query = SalaryAdmin
            queryset_f = queryset_f.filter(type="行政人员")
        elif type == "fangdichan":
            base_query = SalaryFangDiChan
            queryset_f = queryset_f.filter(type="房地产")
        elif type == "shandongdade":
            base_query = SalaryShanDongDade
            queryset_f = queryset_f.filter(type="山东大德")
        else:
            base_query = SalaryJinanPuSheng
            queryset_f = queryset_f.filter(type="济南普晟")
        if queryset_f.count() == 0:
            return ErrorResponse(msg="请先添加公式！")
        # 添加部门后修改20220207
        queryset_f_n = queryset_f.filter(dept__isnull=True).order_by("order")
        queryset_f_y = queryset_f.filter(dept__isnull=False).order_by("order")
        # 没有部门
        for fl in queryset_f_n:
            for query in self.queryset.values():
                if query.get("archive"):
                    continue
                formula = fl.value  # 公式
                formula_name = fl.name  # 公式名
                now_k = ""
                names = {}
                for k, v in query.items():
                    field = base_query._meta.get_field(k)
                    verbose_name = field.verbose_name
                    if self.check_contain_chinese(verbose_name) and verbose_name in formula:
                        names[verbose_name] = str(v.quantize(Decimal("0.00")))

                    if verbose_name == formula_name:
                        now_k = k
                if now_k == "":
                    raise Exception(f"不存在公式名称({formula_name})")
                # 按字符串长->短排序， 比如 ，产假和陪产假，如果产假在前，陪产假就会将产假替换为数字，剩下陪
                name = dict(sorted(names.items(), key=lambda e: len(e[0]), reverse=True))
                for k, v in name.items():
                    formula = formula.replace(k, v)
                if self.check_contain_chinese(formula):
                    chinese = re.compile("[\u4e00-\u9fff]+").findall(formula)
                    return ErrorResponse(msg=f"公式值中字段名称({','.join(chinese)})不存在!")
                try:
                    query[now_k] = eval(formula)
                except ZeroDivisionError as e:
                    print(formula)
                    return ErrorResponse(msg=f"公式值中存在分母为0，{formula}！")
                self.queryset.filter(id=query.get("id")).update(**query)
        # 有部门
        for fl in queryset_f_y:
            dept = Department.objects.filter(id=fl.dept_id)
            if dept.count() == 0:
                continue
            dept_name = dept.first().depart_name
            for query in self.queryset.values():
                if query.get("archive"):
                    continue
                if dept_name != query["dept"]:
                    continue
                formula = fl.value  # 公式
                formula_name = fl.name  # 公式名
                now_k = ""
                names = {}
                for k, v in query.items():
                    field = base_query._meta.get_field(k)
                    verbose_name = field.verbose_name
                    if self.check_contain_chinese(verbose_name) and verbose_name in formula:
                        names[verbose_name] = str(v.quantize(Decimal("0.00")))
                    if verbose_name == formula_name:
                        now_k = k
                # 按字符串长->短排序， 比如 ，产假和陪产假，如果产假在前，陪产假就会将产假替换为数字，剩下陪
                name = dict(sorted(names.items(), key=lambda e: len(e[0]), reverse=True))
                for k, v in name.items():
                    formula = formula.replace(k, v)
                if self.check_contain_chinese(formula):
                    chinese = re.compile("[\u4e00-\u9fff]+").findall(formula)
                    return ErrorResponse(msg=f"公式值中字段名称({','.join(chinese)})不存在!")
                try:
                    query[now_k] = eval(formula)
                except ZeroDivisionError as e:
                    print(formula)
                    return ErrorResponse(msg=f"公式值中存在分母为0，{formula}！")
                self.queryset.filter(id=query.get("id")).update(**query)
        return SuccessResponse({"msg": "计算ok"})

    def archive(self, type, request, *args, **kwargs):
        """
        归档
        {'emp_id__emp_type': '正式', 'emp_id__emp_id': '8002', 'emp_id__emp_name': 'name_2'}
        """
        print("------------", type)
        data = request.data
        if type == "design":
            base_query = SalaryDesign
        elif type == "admin":
            base_query = SalaryAdmin
        elif type == "fangdichan":
            base_query = SalaryFangDiChan
        elif type == "shandongdade":
            base_query = SalaryShanDongDade
        else:
            base_query = SalaryJinanPuSheng

        if "data" in data:
            for dat in data["data"]:
                base_query.objects.filter(archive=0).filter(id=dat["id"]).update(archive=1)
        else:
            depart_name_list = []
            if "dept" in data:
                deptId = data.pop("dept")
                depart_name_list = Department.objects.filter(id__in=deptId).values_list("depart_name", flat=True)
            if data:
                base_query = base_query.objects.filter(archive=0).filter(**data)
            else:
                base_query = base_query.objects.filter(archive=0)
            if depart_name_list:
                base_query = base_query.filter(archive=0).filter(dept__in=depart_name_list)
            base_query.update(archive=1)
        return SuccessResponse()

    def copy(self, type, request, *args, **kwargs):
        """复制"""
        list = []
        if "data" in request.data and request.data.get("data"):
            data = request.data.get("data")
            for dat in data:
                list.append(dat["id"])
        if type == "design":
            base_query = SalaryDesign
            msg = "设计人员"
        elif type == "admin":
            base_query = SalaryAdmin
            msg = "行政人员"
        elif type == "fangdichan":
            base_query = SalaryFangDiChan
            msg = "房地产"
        elif type == "shandongdade":
            base_query = SalaryShanDongDade
            msg = "山东大德"
        else:
            base_query = SalaryJinanPuSheng
            msg = "济南普晟"
        now_time = datetime.datetime.now()
        if list:
            queryset = base_query.objects.filter(id__in=list)
        else:
            queryset = base_query.objects

        # 20220222 F038修改， 添加复制时间以及复制到哪个月的时间
        last_month = request.data.get("before_month")  # 上上月数据
        bf_time = request.data.get("after_month")  # 上个月数据
        if not last_month:
            last_month = self.conversion_time(now_time - relativedelta(months=2))
        if not bf_time:
            bf_time = self.conversion_time(now_time - relativedelta(months=1))

        old_query = queryset.filter(salary_time=bf_time)
        if old_query:
            old_query.delete()
        create_data = []
        new_query = queryset.filter(salary_time=last_month, is_delete=0)
        if new_query.count() == 0:
            raise Exception("找不到%s的数据，未完成复制！" % last_month)
        # 添加清零
        zero_query = Zero.objects.filter(type=msg, is_zero=1)
        zero_list = []
        if zero_query.count() > 0:
            zero_name = zero_query.values_list("name", flat=True)
            for k, v in new_query.values()[0].items():
                field = new_query.model._meta.get_field(k)
                verbose_name = field.verbose_name
                if verbose_name in zero_name:
                    zero_list.append(k)
        for query in new_query.values():
            query["salary_time"] = bf_time
            query.pop("id")
            query.pop("archive")
            query.pop("is_delete")
            query.pop("create_datetime")
            query.pop("description")
            query.pop("creator_id")
            query.pop("modifier")
            query.pop("dept_belong_id")
            query.pop("update_datetime")
            [query.pop(ll) for ll in zero_list]
            create_data.append(base_query(**query))
        queryset.bulk_create(create_data)
        return SuccessResponse()

    def replace(self, type, request, *args, **kwargs):
        """替换"""
        print("------------", type)
        depart_name_list = []
        query = request.data.get("query")
        if "dept" in query:
            deptId = query.pop("dept")
            print("传过来的部门id", deptId)
            depart_name_list = Department.objects.filter(id__in=deptId).values_list("depart_name", flat=True)
        list = []
        if "data" in request.data and request.data.get("data"):
            data = request.data.get("data")
            print("传过来的多选数据id", data)
            for dat in data:
                list.append(dat["id"])
        if type == "design":
            base_query = SalaryDesign
        elif type == "admin":
            base_query = SalaryAdmin
        elif type == "fangdichan":
            base_query = SalaryFangDiChan
        elif type == "shandongdade":
            base_query = SalaryShanDongDade
        else:
            base_query = SalaryJinanPuSheng
        if list:
            base_query = base_query.objects.filter(archive=0).filter(id__in=list)
        elif query:
            print("刨除部门之后的筛选条件",query)
            base_query = base_query.objects.filter(archive=0).filter(**query)
        else:
            base_query = base_query.objects.filter(archive=0)
        print("部门名称列表", depart_name_list)
        if depart_name_list:
            base_query = base_query.filter(archive=0).filter(dept__in=depart_name_list)
        print("最后筛选出来的薪酬数据", base_query)
        region1 = request.data.get("region1")
        olddata1 = request.data.get("olddata1")
        newdata1 = request.data.get("newdata1")
        region2 = request.data.get("region2")
        olddata2 = request.data.get("olddata2")
        newdata2 = request.data.get("newdata2")
        region3 = request.data.get("region3")
        olddata3 = request.data.get("olddata3")
        newdata3 = request.data.get("newdata3")
        s = ["emp_name", "emp_id", "emp_type", "id", "dept", "salary_time"]

        filter_dict = {}

        if region1:
            filter_dict.update(
                {
                    region1: Decimal(olddata1) if region1 not in s else olddata1,
                }
            )

        if region2:
            filter_dict.update(
                {
                    region2: Decimal(olddata2) if region2 not in s else olddata2,
                }
            )

        if region3:
            filter_dict.update(
                {
                    region3: Decimal(olddata3) if region3 not in s else olddata3,
                }
            )

        update_dict = {}
        if newdata1:
            update_dict.update({region1: Decimal(newdata1) if region1 not in s else newdata1})
        if newdata2:
            update_dict.update({region2: Decimal(newdata2) if region2 not in s else newdata2})
        if newdata3:
            update_dict.update({region3: Decimal(newdata3) if region3 not in s else newdata3})

        print("替换之前数据",filter_dict)
        print("替换更新数据",update_dict)
        print("需要替换的数据",base_query)
        print("需要替换的数据", base_query.count())
        queryset = base_query.filter(**filter_dict)
        queryset.update(**update_dict)
        return SuccessResponse()

    def soft_delete(self, type, request, *args, **kwargs):
        print("request.data---", request.data)
        ids = request.data
        if not isinstance(request.data, list):
            ids = [ids]
        self.queryset.filter(id__in=ids, is_delete=0).update(is_delete=1)
        return SuccessResponse()

    def revoke_delete(self, type, request, *args, **kwargs):
        print("request.data撤销---", request.data)
        ids = request.data
        if not isinstance(request.data, list):
            ids = [ids]
        print(self.queryset.all().values())
        print(self.queryset.filter(id__in=ids, is_delete=1).count())
        self.queryset.filter(id__in=ids, is_delete=1).update(is_delete=0)
        return SuccessResponse()

    def sync(self, request, *args, **kwargs):
        resp_sb = self._add_voucher_sb()
        resp_gjj = self._add_voucher_gjj()
        resp_ffgz = self._add_voucher_ffgz()
        resp_fp = self._add_voucher_fp()
        resps = [resp_sb, resp_gjj, resp_ffgz, resp_fp]
        msgs = []
        status = True
        for resp in resps:
            for i in range(3):
                # 加三次请求,只有在requests请求失败的时候重试
                if resp.status_code == 200:
                    resp = resp.json()
                    if resp.get("code") == "200":
                        msgs.append("")
                        print(f"请求成功")
                    else:
                        print(resp.get("message"))
                        msgs.append(resp.get("message"))
                    break
                else:
                    status = False
                    print(f"网络第{str(i + 1)}次请求失败")
                    if i == 2:
                        # 只添加最后一次msg
                        msgs.append("网络请求失败！")
                time.sleep(0.5)
        if status:
            return SuccessResponse()
        else:
            msgs = [msg for msg in msgs if msg]
            return SuccessResponse(",".join(msgs))

    # def sync(self, request, *args, **kwargs):
    #     access_token = self._get_access_token()
    #     if not access_token:
    #         raise Exception("无access_token,请联系管理员！")
    #     # resp_sb = self._add_voucher_sb(access_token)
    #     # resp_gjj = self._add_voucher_gjj(access_token)
    #     resp_ffgz = self._add_voucher_ffgz(access_token)
    #     # resp_fp = self._add_voucher_fp(access_token)
    #     resp = resp_ffgz.json()
    #     print(resp)
    #     if resp.get("code") == "200":
    #         return SuccessResponse()
    #     else:
    #         return SuccessResponse(resp.get("message"))

    def excel_to_data(self, file_url, field_data):
        """
        仅仅是重写这个函数的读取路径
        """
        # 读取excel 文件
        sys = platform.system()
        if sys == "Windows":
            path = os.getcwd() + file_url
        else:
            path = os.path.join(settings.BASE_DIR.replace("\\", os.sep), *file_url.split(os.sep))
        data = xlrd.open_workbook(path)
        table = data.sheets()[0]
        # 创建一个空列表，存储Excel的数据
        tables = []
        for i, rown in enumerate(range(table.nrows)):
            if i == 0:
                continue
            array = {}
            for index, ele in enumerate(field_data.keys()):
                cell_value = table.cell_value(rown, index)
                # 由于excel导入数字类型后，会出现数字加 .0 的，进行处理
                if type(cell_value) is float and str(cell_value).split(".")[1] == "0":
                    cell_value = int(str(cell_value).split(".")[0])
                if type(cell_value) is str:
                    cell_value = cell_value.strip(" \t\n\r")
                array[ele] = cell_value
            tables.append(array)
        return tables

    def importTemplate(self, request: Request, *args, **kwargs):
        assert self.import_field_data, "'%s' 请配置对应的导出模板字段。" % self.__class__.__name__
        # 导出模板
        if request.method == "GET":
            # 示例数据
            queryset = self.filter_queryset(self.get_queryset())
            return SuccessResponse(
                export_excel_save_model(
                    request,
                    self.import_field_data.values(),
                    [],
                    f"导入{get_verbose_name(queryset)}模板.xls",
                )
            )
        updateSupport = request.data.get("updateSupport")
        # 从excel中组织对应的数据结构，然后使用序列化器保存
        data = self.excel_to_data(request.data.get("file_url"), self.import_field_data)
        queryset = self.filter_queryset(self.get_queryset())
        unique_list = [
            ele.attname for ele in queryset.model._meta.get_fields() if hasattr(ele, "unique") and ele.unique == True
        ]
        unique_list.append("idcard_number")
        unique_list.append("salary_time")
        for ele in data:
            new_ele = {}
            # excel中不填写则不导入，填写0要导入
            # 之前的写法  ele = {k: v for k, v in ele.items() if v}
            for k, v in ele.items():
                if not v and v != 0:
                    continue
                elif v and not isinstance(v, str):
                    new_ele.update({k: round(v, 2)})
                else:
                    new_ele.update({k: v})

            # 获取 unique 字段
            filter_dic = {i: str(new_ele.get(i)) for i in list(set(self.import_field_data.keys()) & set(unique_list))}
            instance = filter_dic and queryset.filter(**filter_dic).first()
            if instance and not updateSupport:
                raise Exception("%s已经存在，请在导入界面勾选更新选项。" % new_ele.get("idcard_number"))
            if not filter_dic:
                instance = None
            serializer = self.import_serializer_class(instance, data=new_ele, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.errors and print(serializer.errors)
            serializer.save()
        return SuccessResponse(msg=f"导入成功！")

    def _get_signature(self, secret, data):
        secret = secret.encode("utf-8")
        data = data.encode("utf-8")
        return base64.b64encode(hmac.new(secret, data, digestmod=sha256).digest())

    def _get_access_token(self, type, appKey, appSecret):
        """获取上传数据所需的access_token"""
        now_time = time.time()
        query = SyncAccessToken.objects.filter(type=type)
        if query.count() > 0 and query.first().now_time + query.first().expire > now_time:
            access_token = query.first().access_token
        else:
            url = "https://open.yonyoucloud.com/open-auth/selfAppAuth/getAccessToken"
            timestamp = str(int(time.time() * 1000))
            params = {
                "appKey": appKey,
                "timestamp": timestamp,
                "signature": self._get_signature(appSecret, "appKey" + appKey + "timestamp" + timestamp),
            }
            resp = requests.get(url=url, params=params)
            if resp.status_code == 200:
                try:
                    json_data = json.loads(resp.text).get("data")
                    access_token = json_data.get("access_token")
                    create_data = {
                        "access_token": access_token,
                        "now_time": now_time,
                        "expire": json_data.get("expire"),
                        "type": str(type),
                    }
                    if query.count() > 0:
                        SyncAccessToken.objects.update(**create_data)
                    else:
                        SyncAccessToken.objects.create(**create_data)
                except:
                    access_token = ""
            else:
                access_token = ""
        return access_token

    def del_zero(self, data: list):
        lt = []
        for ll in data:
            if "debitOrg" in ll:
                if ll.get("debitOrg") > 0:
                    lt.append(ll)
            if "creditOrg" in ll:
                if ll.get("creditOrg") > 0:
                    lt.append(ll)
        return lt

    def _add_voucher_sb(self):
        """社保"""
        key_secret = self.sync_voucher["ff"]
        access_token = self._get_access_token("ff", key_secret.get("key"), key_secret.get("secret"))
        if not access_token:
            raise Exception("无access_token,请联系管理员！")

        mobile = key_secret.get("mobile")
        billTime = datetime.datetime.strftime(datetime.datetime.now() - relativedelta(months=1), "%Y-%m-%d")
        time_split = billTime.split("-")
        salary_time = f"{time_split[0]}-{time_split[1]}"
        description = f"缴纳{time_split[0]}.{time_split[1]}月员工社保"

        sum_shebao_d = 0
        for i in CompanyBear.objects.filter(salary_time=salary_time).values("shebao"):
            sum_shebao_d += i.get("shebao")

        sum_shebao_design = 0
        for i in SalaryDesign.objects.filter(is_delete=0, salary_time=salary_time).values("social_security"):
            sum_shebao_design += i.get("social_security")

        sum_shebao_admin = 0
        for i in SalaryAdmin.objects.filter(is_delete=0, salary_time=salary_time).values("social_security"):
            sum_shebao_admin += i.get("social_security")

        sum_shebao_g = sum_shebao_design + sum_shebao_admin

        bodies = [
            {
                "description": description,  # 摘要    示例：购买**设备
                "accsubjectCode": "221107",  # 科目编码
                "debitOriginal": float(sum_shebao_d),  # 原币借方金额（借贷不能同时填写，原币本币都要填写）
                "debitOrg": float(sum_shebao_d),  # 本币借方金额（借贷不能同时填写，原币本币都要填写）
                "rateType": "01",  # 汇率类型（01基准类型，02自定义类型
                # "settlementModeCode": "system_0001",  # 结算方式code
                "billTime": billTime,  # 票据日期    示例：2021-08-23
                # "billNo": "10001",  # 票据号
                # "bankVerifyCode": "20001",  # 银行对账码
            },
            {
                "description": description,
                "accsubjectCode": "122103",
                "debitOriginal": float(sum_shebao_g),
                "debitOrg": float(sum_shebao_g),
                "rateType": "01",
                "billTime": billTime,
            },
        ]
        daikou = {}
        queryset = PayBehalf.objects.filter(salary_time=salary_time)
        for query in queryset:
            is_company = 1
            if query.summary_name in daikou:
                if daikou[query.summary_name]["is_company"] == 1 and query.summary_name != query.name:
                    is_company = 0
                daikou[query.summary_name] = {
                    "shebao": daikou[query.summary_name]["shebao"] + query.shebao,
                    "is_company": is_company,
                }
            else:
                if query.summary_name != query.name:
                    is_company = 0
                daikou[query.summary_name] = {
                    "shebao": query.shebao,
                    "is_company": is_company,
                }
        INDEX = 61
        credit = 0
        for k, v in daikou.items():
            credit += float(v["shebao"])
            if v["is_company"]:
                # 单位
                accsubjectCode = "122101"
            else:
                # 个人
                accsubjectCode = "122102"
            dt = {
                "description": description,
                "accsubjectCode": accsubjectCode,
                "debitOriginal": float(v["shebao"]),
                "debitOrg": float(v["shebao"]),
                "rateType": "01",
                "billTime": billTime,
                "clientAuxiliaryList": [{"filedCode": "0005", "valueCode": "000000" + str(INDEX)}],  # 客户
            }
            bodies.append(dt)
            INDEX += 1
        bodies.append(
            {
                "description": description,
                "accsubjectCode": "100207",
                "creditOriginal": round(credit + float(sum_shebao_d) + float(sum_shebao_g), 2),
                "creditOrg": round(credit + float(sum_shebao_d) + float(sum_shebao_g), 2),
                "rateType": "01",
                "billTime": billTime,
                "cashflowList": [
                    {
                        "mainItemCode": "1122",
                        "amountOriginal": round(credit + float(sum_shebao_d) + float(sum_shebao_g), 2),
                        "amountOrg": round(credit + float(sum_shebao_d) + float(sum_shebao_g), 2),
                    }
                ],
            }
        )
        post_data = {
            "srcSystemCode": "hr_cloud",  # 来源系统  人力资源：hr_cloud
            "accbookCode": "9999_0001",  # 账簿code
            "voucherTypeCode": "1",  # 凭证类型code
            "makerMobile": str(mobile),  # 制单人手机号（手机号和邮箱不能同时为空）
            # 'makerEmail': '',
            "bodies": self.del_zero(bodies),
        }
        resp = requests.post(
            url=self.url,
            params={"access_token": access_token},
            data=json.dumps(post_data),
        )
        return resp

    def _add_voucher_gjj(self):
        """公积金"""
        key_secret = self.sync_voucher["ff"]
        access_token = self._get_access_token("ff", key_secret.get("key"), key_secret.get("secret"))
        if not access_token:
            raise Exception("无access_token,请联系管理员！")

        mobile = key_secret.get("mobile")

        billTime = datetime.datetime.strftime(datetime.datetime.now() - relativedelta(months=1), "%Y-%m-%d")
        time_split = billTime.split("-")
        salary_time = f"{time_split[0]}-{time_split[1]}"
        description = f"缴纳{time_split[0]}.{time_split[1]}月员工公积金"

        sum_gongjijin_d = 0
        for i in CompanyBear.objects.filter(salary_time=salary_time).values("gongjijin"):
            sum_gongjijin_d += i.get("gongjijin")

        sum_gongjijin_design = 0
        for i in SalaryDesign.objects.filter(is_delete=0, salary_time=salary_time).values("accumulation_fund"):
            sum_gongjijin_design += i.get("accumulation_fund")

        sum_gongjijin_admin = 0
        for i in SalaryAdmin.objects.filter(is_delete=0, salary_time=salary_time).values("accumulation_fund"):
            sum_gongjijin_admin += i.get("accumulation_fund")

        sum_gongjijin_g = sum_gongjijin_design + sum_gongjijin_admin

        bodies = [
            {
                "description": description,  # 摘要    示例：购买**设备
                "accsubjectCode": "221105",  # 科目编码
                "debitOriginal": float(sum_gongjijin_d),  # 原币借方金额（借贷不能同时填写，原币本币都要填写）
                "debitOrg": float(sum_gongjijin_d),  # 本币借方金额（借贷不能同时填写，原币本币都要填写）
                "rateType": "01",  # 汇率类型（01基准类型，02自定义类型
                # "settlementModeCode": "system_0001",  # 结算方式code
                "billTime": billTime,  # 票据日期    示例：2021-08-23
                # "billNo": "10001",  # 票据号
                # "bankVerifyCode": "20001",  # 银行对账码
            },
            {
                "description": description,
                "accsubjectCode": "122106",
                "debitOriginal": float(sum_gongjijin_g),
                "debitOrg": float(sum_gongjijin_g),
                "rateType": "01",
                "billTime": billTime,
            },
        ]
        daikou = {}
        queryset = PayBehalf.objects.filter(salary_time=salary_time)
        for query in queryset:
            is_company = 1
            if query.summary_name in daikou:
                if daikou[query.summary_name]["is_company"] == 1 and query.summary_name != query.name:
                    is_company = 0
                daikou[query.summary_name] = {
                    "gongjijin": daikou[query.summary_name]["gongjijin"] + query.gongjijin,
                    "is_company": is_company,
                }
            else:
                if query.summary_name != query.name:
                    is_company = 0
                daikou[query.summary_name] = {
                    "gongjijin": query.gongjijin,
                    "is_company": is_company,
                }
        INDEX = 61
        credit = 0
        for k, v in daikou.items():
            credit += float(v["gongjijin"])
            if v["is_company"]:
                # 单位
                accsubjectCode = "122101"
            else:
                # 个人
                accsubjectCode = "122102"
            dt = {
                "description": description,
                "accsubjectCode": accsubjectCode,
                "debitOriginal": float(v["gongjijin"]),
                "debitOrg": float(v["gongjijin"]),
                "rateType": "01",
                "billTime": billTime,
                "clientAuxiliaryList": [{"filedCode": "0005", "valueCode": "000000" + str(INDEX)}],  # 客户
            }
            bodies.append(dt)
            INDEX += 1
        bodies.append(
            {
                "description": description,
                "accsubjectCode": "100207",
                "creditOriginal": credit + float(sum_gongjijin_d) + float(sum_gongjijin_g),
                "creditOrg": credit + float(sum_gongjijin_d) + float(sum_gongjijin_g),
                "rateType": "01",
                "billTime": billTime,
                "cashflowList": [
                    {
                        "mainItemCode": "1122",
                        "amountOriginal": credit + float(sum_gongjijin_d) + float(sum_gongjijin_g),
                        "amountOrg": credit + float(sum_gongjijin_d) + float(sum_gongjijin_g),
                    }
                ],
            }
        )
        post_data = {
            "srcSystemCode": "hr_cloud",  # 来源系统  人力资源：hr_cloud
            "accbookCode": "9999_0001",  # 账簿code
            "voucherTypeCode": "1",  # 凭证类型code
            "makerMobile": str(mobile),  # 制单人手机号（手机号和邮箱不能同时为空）
            # 'makerEmail': '',
            "bodies": self.del_zero(bodies),
        }
        resp = requests.post(
            url=self.url,
            params={"access_token": access_token},
            data=json.dumps(post_data),
        )
        return resp

    def _add_voucher_ffgz(self):
        """发放工资"""
        # 应发合计， 代扣车位，应扣餐费， 社保，公积金，个人所得税
        key_secret = self.sync_voucher["ff"]
        access_token = self._get_access_token("ff", key_secret.get("key"), key_secret.get("secret"))
        if not access_token:
            raise Exception("无access_token,请联系管理员！")

        mobile = key_secret.get("mobile")
        billTime = datetime.datetime.strftime(datetime.datetime.now() - relativedelta(months=1), "%Y-%m-%d")
        time_split = billTime.split("-")
        salary_time = f"{time_split[0]}-{time_split[1]}"
        description = f"发放{time_split[0]}.{time_split[1]}月工资"

        design = SalaryDesign.objects.filter(is_delete=0, salary_time=salary_time)
        admin = SalaryAdmin.objects.filter(is_delete=0, salary_time=salary_time)
        ffgz = {
            "total_payable": Decimal(0),
            "parking": Decimal(0),
            "meals": Decimal(0),
            "social_security": Decimal(0),
            "accumulation_fund": Decimal(0),
            "individual_income_tax": Decimal(0),
            "agent_deduct": Decimal(0),
        }
        for querys in [design, admin]:
            for query in querys:
                ffgz["total_payable"] += query.total_payable
                ffgz["parking"] += query.parking
                ffgz["meals"] += query.meals
                ffgz["social_security"] += query.social_security
                ffgz["accumulation_fund"] += query.accumulation_fund
                ffgz["individual_income_tax"] += query.individual_income_tax
                ffgz["agent_deduct"] += query.agent_deduct
        cb_query = CompanyBear.objects.filter(salary_time=salary_time)
        pb_query = PayBehalf.objects.filter(salary_time=salary_time)
        for queryset in [cb_query, pb_query]:
            for query in queryset:
                ffgz["total_payable"] += query.gongjijin + query.shebao
        ffgz["issued_total"] = (
            ffgz["total_payable"]
            - ffgz["parking"]
            - ffgz["meals"]
            - ffgz["social_security"]
            - ffgz["accumulation_fund"]
            - ffgz["individual_income_tax"]
            - ffgz["agent_deduct"]
        )
        bodies = [
            {
                "description": description,  # 摘要    示例：购买**设备
                # "accsubjectCode": '122101',  # 科目编码
                "accsubjectCode": "224104",  # 科目编码
                "debitOriginal": float(ffgz["total_payable"]),  # 原币借方金额（借贷不能同时填写，原币本币都要填写）
                "debitOrg": float(ffgz["total_payable"]),  # 本币借方金额（借贷不能同时填写，原币本币都要填写）
                "rateType": "01",  # 汇率类型（01基准类型，02自定义类型
                # "settlementModeCode": "system_0001",  # 结算方式code
                "billTime": billTime,  # 票据日期    示例：2021-08-23
                # "billNo": "10001",  # 票据号
                # "bankVerifyCode": "20001",  # 银行对账码
            },
            {
                "description": description,
                "accsubjectCode": "224105",
                "creditOriginal": float(ffgz["parking"]),
                "creditOrg": float(ffgz["parking"]),
                "rateType": "01",
                "billTime": billTime,
            },
            {
                "description": description,
                "accsubjectCode": "224102",
                "creditOriginal": float(ffgz["meals"]),
                "creditOrg": float(ffgz["meals"]),
                "rateType": "01",
                "billTime": billTime,
            },
            {
                "description": description,
                "accsubjectCode": "224106",
                "creditOriginal": float(ffgz["agent_deduct"]),
                "creditOrg": float(ffgz["agent_deduct"]),
                "rateType": "01",
                "billTime": billTime,
            },
            {
                "description": description,
                "accsubjectCode": "122105",
                "creditOriginal": float(ffgz["social_security"]),
                "creditOrg": float(ffgz["social_security"]),
                "rateType": "01",
                "billTime": billTime,
            },
            {
                "description": description,
                "accsubjectCode": "122106",
                "creditOriginal": float(ffgz["accumulation_fund"]),
                "creditOrg": float(ffgz["accumulation_fund"]),
                "rateType": "01",
                "billTime": billTime,
            },
            {
                "description": description,
                "accsubjectCode": "222141",
                "creditOriginal": float(ffgz["individual_income_tax"]),
                "creditOrg": float(ffgz["individual_income_tax"]),
                "rateType": "01",
                "billTime": billTime,
                # "cashflowList": [
                #     {
                #         "mainItemCode": "1122",
                #         "amountOriginal": float(ffgz['individual_income_tax']),
                #         "amountOrg": float(ffgz['individual_income_tax']),
                #     }
                # ]
            },
            {
                "description": description,
                "accsubjectCode": "100208",
                "creditOriginal": float(ffgz["issued_total"]),
                "creditOrg": float(ffgz["issued_total"]),
                "rateType": "01",
                "billTime": billTime,
                "cashflowList": [
                    {
                        "mainItemCode": "1122",
                        "amountOriginal": float(ffgz["issued_total"]),
                        "amountOrg": float(ffgz["issued_total"]),
                    }
                ],
            },
        ]

        post_data = {
            "srcSystemCode": "hr_cloud",  # 来源系统  人力资源：hr_cloud
            "accbookCode": "9999_0001",  # 账簿code
            "voucherTypeCode": "1",  # 凭证类型code
            "makerMobile": str(mobile),  # 制单人手机号（手机号和邮箱不能同时为空）
            # 'makerEmail': '',
            "bodies": self.del_zero(bodies),
        }
        resp = requests.post(
            url=self.url,
            params={"access_token": access_token},
            data=json.dumps(post_data),
        )
        return resp

    def _add_voucher_fp(self):
        """分配工资社保公积金"""
        key_secret = self.sync_voucher["fp"]
        access_token = self._get_access_token("ff", key_secret.get("key"), key_secret.get("secret"))
        if not access_token:
            raise Exception("无access_token,请联系管理员！")

        mobile = key_secret.get("mobile")

        billTime = datetime.datetime.strftime(datetime.datetime.now() - relativedelta(months=1), "%Y-%m-%d")
        time_split = billTime.split("-")
        salary_time = f"{time_split[0]}-{time_split[1]}"
        description = f"分配{time_split[0]}年{time_split[1]}月份工资及各项福利费用"

        admin_query = SalaryAdmin.objects.filter(is_delete=0, salary_time=salary_time)
        design_query = SalaryDesign.objects.filter(is_delete=0, salary_time=salary_time)
        """
        行政人员的工资、社保、公积金汇总成管理费用/工资及附加/工资及福利，
        设计人员中非研发人员的汇总成主营业务成本/工资及附加。。。按部门汇总成几行。
        设计人员中研发人员，则汇总成管理费用/研发费用/人工费，也是按部门汇总。
        """
        admin_dt = {}  # 行政人员的工资、社保、公积金

        yanfa = {}
        feiyanfa = {}
        shebao = 0
        gongjijin = 0
        gongzi = 0  # 实发工资
        cd_shebao = 0
        departments = []

        for query in admin_query:
            compute = query.total_payable
            dept_query = Department.objects.filter(depart_name=query.dept)
            if dept_query.count() == 0:
                continue
            queryset = CompanyBear.objects.filter(dept=dept_query.first().id, salary_time=query.salary_time)
            qt = queryset.first()
            if queryset.count() > 0 and qt.id not in departments:
                compute += qt.shebao + qt.gongjijin
                # 贷方 公司承担
                cd_shebao += qt.shebao
                gongjijin += qt.gongjijin
                departments.append(qt.id)
            if query.dept in admin_dt:
                admin_dt[query.dept] += compute
            else:
                admin_dt[query.dept] = compute

            # 贷方
            shebao += query.social_security
            gongjijin += query.accumulation_fund
            gongzi += query.total_payable - query.social_security - query.accumulation_fund

        print(cd_shebao)
        print(shebao)
        for query in design_query:
            compute = query.total_payable
            dept_query = Department.objects.filter(depart_name=query.dept)
            if dept_query.count() == 0:
                continue
            queryset = CompanyBear.objects.filter(dept=dept_query.first().id, salary_time=query.salary_time)
            qt = queryset.first()
            if queryset.count() > 0 and qt.id not in departments:
                compute += qt.shebao + qt.gongjijin
                # 贷方
                cd_shebao += qt.shebao
                gongjijin += qt.gongjijin
                departments.append(qt.id)
            if "研发" in query.emp_type:
                if query.dept in yanfa:
                    yanfa[query.dept] += compute
                else:
                    yanfa[query.dept] = compute
            else:
                if query.dept in feiyanfa:
                    feiyanfa[query.dept] += compute
                else:
                    feiyanfa[query.dept] = compute

            # 贷方
            shebao += query.social_security
            gongjijin += query.accumulation_fund
            gongzi += query.total_payable - query.social_security - query.accumulation_fund
        print(cd_shebao)
        bodies = []
        for index, query in enumerate([admin_dt, yanfa, feiyanfa]):
            if index == 0:
                accsubjectCode = "66020105"
            elif index == 1:
                accsubjectCode = "66021701"
            else:
                accsubjectCode = "64010105"
            for k, v in query.items():
                dt = {
                    "description": description,
                    "accsubjectCode": accsubjectCode,
                    "debitOriginal": float(v),
                    "debitOrg": float(v),
                    "rateType": "01",
                    "billTime": billTime,
                    "clientAuxiliaryList": [
                        {
                            "filedCode": "0001",
                            # "valueCode": self.dept_code[k]
                            "valueCode": "cs0001",
                        }
                    ],
                }
                bodies.append(dt)
        bodies += [
            {
                "description": description,
                "accsubjectCode": "221101",
                "creditOriginal": float(gongzi),
                "creditOrg": float(gongzi),
                "rateType": "01",
                "billTime": billTime,
            },
            {
                "description": description,
                "accsubjectCode": "221107",
                "creditOriginal": float(shebao + cd_shebao),
                "creditOrg": float(shebao + cd_shebao),
                "rateType": "01",
                "billTime": billTime,
            },
            {
                "description": description,
                "accsubjectCode": "221106",
                "creditOriginal": float(gongjijin),
                "creditOrg": float(gongjijin),
                "rateType": "01",
                "billTime": billTime,
            },
        ]
        post_data = {
            "srcSystemCode": "hr_cloud",  # 来源系统  人力资源：hr_cloud
            "accbookCode": "9999_0001",  # 账簿code
            "voucherTypeCode": "1",  # 凭证类型code
            "makerMobile": str(mobile),  # 制单人手机号（手机号和邮箱不能同时为空）
            # 'makerEmail': '',
            "bodies": self.del_zero(bodies),
        }
        resp = requests.post(
            url=self.url,
            params={"access_token": access_token},
            data=json.dumps(post_data),
        )
        return resp
