from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from .functions import *


def data_lineage_landing(request):
    context = {'json_data': get_metadata()}
    return render(request, 'data_observability/data-lineage.html', context)


@csrf_exempt
def create_data_lineage(request):
    if request.method == 'POST':
        d = request.POST
        result = generate_visjs_data(d.get('db_name'), d.get('schema_name'), d.get('table_name'))
        return JsonResponse(result)


@csrf_exempt
def expand_data_lineage(request):
    if request.method == 'POST':
        d = request.POST
        result = expand_visjs_data(d.get('table_name'), d.get('node_id'), d.get('node_level'), d.get('connected_nodes'))
        return JsonResponse(result)


@csrf_exempt
def expand_data_lineage_back(request):
    if request.method == 'POST':
        d = request.POST
        result = expand_visjs_data_back(d.get('table_name'), d.get('node_id'), d.get('node_level'), d.get('connected_nodes'))
        return JsonResponse(result)


@csrf_exempt
def display_edge_dtls(request):
    if request.method == 'POST':
        d = request.POST
        result = get_data_definition(d.get('source'), d.get('target'), d.get('mapping'))
        result['description'].fillna("NA", inplace=True)
        result['column_type'].fillna("NA", inplace=True)
        result['column_logic'].fillna("NA", inplace=True)
        return JsonResponse({'tableData': result.to_dict('records')})
