from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required, permission_required
from django.contrib.auth import get_user_model
from django.http import HttpResponse, JsonResponse
import json
from .functions import *




@login_required(login_url="/login/")
def data_observability_landing(request):
    context = {'segment': 'observability'}
    if request.method == 'GET':
        return render(request, 'data_observability/data-observability.html', context)
   


@login_required(login_url="/login/")
def data_lineage_landing(request):
    context = {'segment': 'observability'}
    context['json_data'] = get_metadata()
    if request.method == 'GET':
        return render(request, 'data_observability/data-lineage.html', context)
   
@login_required(login_url="/login/")
def create_data_lineage(request):
    if request.method == 'POST':
        requestDict = request.POST.dict()
        result = generate_visjs_data(requestDict.get('db_name'),requestDict.get('schema_name'),requestDict.get('table_name'))
        return JsonResponse(result)

@login_required(login_url="/login/")
def expand_data_lineage(request):
    if request.method == 'POST':
        requestDict = request.POST.dict()
        result = expand_visjs_data(requestDict.get('table_name'), requestDict.get('node_id'), requestDict.get('node_level'), requestDict.get('connected_nodes'))
        return JsonResponse(result)
   
@login_required(login_url="/login/")
def expand_data_lineage_back(request):
    if request.method == 'POST':
        requestDict = request.POST.dict()
        result = expand_visjs_data_back(requestDict.get('table_name'), requestDict.get('node_id'), requestDict.get('node_level'), requestDict.get('connected_nodes'))
        return JsonResponse(result)
   
@login_required(login_url="/login/")
def display_edge_dtls(request):
    if request.method == 'POST':
        requestDict = request.POST.dict()
        result = get_data_definition(requestDict.get('source'), requestDict.get('target'), requestDict.get('mapping'))
        result['description'].fillna("NA", inplace=True)
        result['column_type'].fillna("NA", inplace=True)
        result['column_logic'].fillna("NA", inplace=True)
        resp = {'tableData': result.to_dict('records')}
        return JsonResponse(resp)