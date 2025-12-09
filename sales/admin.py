"""Admin configuration for MLS Sales Dashboard.

This module registers models with the Django admin site and configures
their display and filtering options.
"""

from django.contrib import admin

from .models import AgentStats, Member, Property, SyncLog


@admin.register(Member)
class MemberAdmin(admin.ModelAdmin):
    """Admin configuration for Member model."""

    list_display = [
        "member_full_name",
        "member_aor",
        "office_name",
        "member_preferred_phone",
        "member_status",
    ]
    list_filter = [
        "member_aor",
        "member_status",
        "member_type",
    ]
    search_fields = [
        "member_full_name",
        "member_first_name",
        "member_last_name",
        "member_mls_id",
        "office_name",
    ]
    ordering = ["member_full_name"]


@admin.register(Property)
class PropertyAdmin(admin.ModelAdmin):
    """Admin configuration for Property model."""

    list_display = [
        "unparsed_address",
        "city",
        "standard_status",
        "close_price",
        "close_date",
        "list_agent_full_name",
        "buyer_agent_full_name",
    ]
    list_filter = [
        "standard_status",
        "property_type",
        "city",
        "list_agent_aor",
    ]
    search_fields = [
        "unparsed_address",
        "street_name",
        "city",
        "listing_id",
        "list_agent_full_name",
        "buyer_agent_full_name",
    ]
    date_hierarchy = "close_date"
    ordering = ["-close_date"]


@admin.register(SyncLog)
class SyncLogAdmin(admin.ModelAdmin):
    """Admin configuration for SyncLog model."""

    list_display = [
        "sync_type",
        "status",
        "started_at",
        "completed_at",
        "records_processed",
        "records_created",
        "records_updated",
    ]
    list_filter = [
        "sync_type",
        "status",
    ]
    ordering = ["-started_at"]
    readonly_fields = [
        "sync_type",
        "started_at",
        "completed_at",
        "records_processed",
        "records_created",
        "records_updated",
        "status",
        "error_message",
        "last_modification_timestamp",
    ]


@admin.register(AgentStats)
class AgentStatsAdmin(admin.ModelAdmin):
    """Admin configuration for AgentStats model."""

    list_display = [
        "member",
        "year",
        "aor",
        "total_volume",
        "transaction_count",
        "rank_overall",
        "rank_in_aor",
    ]
    list_filter = [
        "year",
        "aor",
    ]
    search_fields = [
        "member__member_full_name",
    ]
    ordering = ["-total_volume"]

