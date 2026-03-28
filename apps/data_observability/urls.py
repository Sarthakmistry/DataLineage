from django.urls import path
from apps.data_observability import views

urlpatterns = [
    path('', views.data_lineage_landing, name='data-lineage'),
    path('create-lineage/', views.create_data_lineage, name='create-lineage'),
    path('expand-lineage/', views.expand_data_lineage, name='expand-lineage'),
    path('expandback-lineage/', views.expand_data_lineage_back, name='expand-lineage-back'),
    path('display-edge/', views.display_edge_dtls, name='edge-info'),
]
