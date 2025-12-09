"""URL configuration for sales app.

This module defines URL patterns for the MLS Sales Dashboard views.
"""

from django.urls import path

from . import views

app_name = "sales"

urlpatterns = [
    # Dashboard
    path("", views.DashboardView.as_view(), name="dashboard"),
    # Leaderboard
    path("leaderboard/", views.LeaderboardView.as_view(), name="leaderboard"),
    # Agents
    path("agents/", views.AgentListView.as_view(), name="agent_list"),
    path(
        "agents/<int:member_key>/",
        views.AgentDetailView.as_view(),
        name="agent_detail",
    ),
    # Properties
    path("properties/", views.PropertyListView.as_view(), name="property_list"),
    path(
        "properties/<int:listing_key>/",
        views.PropertyDetailView.as_view(),
        name="property_detail",
    ),
]

