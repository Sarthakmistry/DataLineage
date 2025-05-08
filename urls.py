from django.urls import path, re_path
from apps.data_observability import views


urlpatterns = [
    path('data-observability/', views.data_observability_landing, name='data-observability'),
    path('data-observability/data-lineage/', views.data_lineage_landing, name='data-lineage'),
    path('data-observability/data-lineage/create-lineage/', views.create_data_lineage, name='create-lineage'),
    path('data-observability/data-lineage/expand-lineage/', views.expand_data_lineage, name='expand-lineage'),
    path('data-observability/data-lineage/expandback-lineage/', views.expand_data_lineage_back, name='expand-lineage-back'),
    path('data-observability/data-lineage/display-edge/', views.display_edge_dtls, name='edge-info'),








]
