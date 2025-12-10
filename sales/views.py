"""Views for MLS Sales Dashboard.

This module contains views for displaying leaderboards, agent directories,
and property searches.
"""

from decimal import Decimal
from typing import Any

from django.db.models import Q, QuerySet, Sum
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.views.generic import DetailView, ListView, TemplateView

from .filters import AgentStatsFilter, MemberFilter, PropertyFilter
from .models import AgentStats, Member, Property, SyncLog


class DashboardView(TemplateView):
    """Home dashboard view with summary statistics."""

    template_name = "dashboard.html"

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        """Get context data for the dashboard.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Context dictionary with summary stats.
        """
        context = super().get_context_data(**kwargs)
        current_year = timezone.now().year

        # Get counts
        context["total_agents"] = Member.objects.count()
        context["total_properties"] = Property.objects.filter(
            close_date__year=current_year
        ).count()
        context["closed_properties"] = Property.objects.filter(
            standard_status="Closed",
            close_date__year=current_year,
        ).count()

        # Get total volume
        total_volume = Property.objects.filter(
            standard_status="Closed",
            close_date__year=current_year,
        ).aggregate(total=Sum("close_price"))["total"]
        context["total_volume"] = Decimal(str(total_volume or 0))

        # Get top 5 agents
        context["top_agents"] = AgentStats.objects.filter(
            year=current_year
        ).select_related("member").order_by("-total_volume")[:5]

        # Get recent sync info
        context["last_sync"] = SyncLog.objects.filter(
            status=SyncLog.SyncStatus.COMPLETED
        ).first()

        # Get unique AORs for quick links
        context["aors"] = (
            AgentStats.objects.filter(year=current_year)
            .values_list("aor", flat=True)
            .distinct()
            .order_by("aor")
        )

        context["current_year"] = current_year

        return context


class LeaderboardView(ListView):
    """Leaderboard view showing top agents by sales volume."""

    model = AgentStats
    template_name = "leaderboard.html"
    context_object_name = "agent_stats"
    paginate_by = 50

    def get_queryset(self) -> QuerySet[AgentStats]:
        """Get filtered and ordered queryset.

        Orders by AOR then rank_in_aor when showing all AORs,
        or just by rank_in_aor when filtered to a specific AOR.

        Returns:
            Filtered AgentStats queryset ordered by rank within AOR.
        """
        queryset = AgentStats.objects.select_related("member")

        # Apply filters first
        self.filterset = AgentStatsFilter(self.request.GET, queryset=queryset)
        filtered_qs = self.filterset.qs

        # Order by AOR first if no specific AOR filter is applied,
        # then by rank within AOR
        aor_filter = self.request.GET.get("aor")
        if aor_filter:
            # When filtered to specific AOR, just order by rank
            return filtered_qs.order_by("rank_in_aor", "-total_volume")
        else:
            # When showing all AORs, group by AOR then order by rank
            return filtered_qs.order_by("aor", "rank_in_aor", "-total_volume")

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        """Get context data for the leaderboard.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Context dictionary with filter and stats.
        """
        context = super().get_context_data(**kwargs)
        context["filterset"] = self.filterset
        context["current_year"] = timezone.now().year

        # Calculate summary stats for filtered results
        filtered_qs = self.filterset.qs
        context["total_agents_shown"] = filtered_qs.count()
        context["total_volume_shown"] = filtered_qs.aggregate(
            total=Sum("total_volume")
        )["total"] or Decimal("0")
        context["total_transactions_shown"] = filtered_qs.aggregate(
            total=Sum("transaction_count")
        )["total"] or 0

        return context


class AgentListView(ListView):
    """Agent directory view with search and filtering."""

    model = Member
    template_name = "agents.html"
    context_object_name = "agents"
    paginate_by = 25

    def get_queryset(self) -> QuerySet[Member]:
        """Get filtered and ordered queryset.

        Returns:
            Filtered Member queryset.
        """
        queryset = Member.objects.filter(
            member_status="Active"
        ).order_by("member_full_name")

        # Apply filters
        self.filterset = MemberFilter(self.request.GET, queryset=queryset)
        return self.filterset.qs

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        """Get context data for the agent list.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Context dictionary with filter.
        """
        context = super().get_context_data(**kwargs)
        context["filterset"] = self.filterset
        context["total_agents"] = self.filterset.qs.count()
        return context


class AgentDetailView(DetailView):
    """Agent detail view with transaction history and stats."""

    model = Member
    template_name = "agent_detail.html"
    context_object_name = "agent"
    slug_field = "member_key_numeric"
    slug_url_kwarg = "member_key"

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        """Get context data for the agent detail.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Context dictionary with agent stats and transactions.
        """
        context = super().get_context_data(**kwargs)
        agent = self.object
        current_year = timezone.now().year

        # Get agent stats
        context["stats"] = AgentStats.objects.filter(
            member=agent
        ).order_by("-year")

        # Get current year stats
        context["current_stats"] = AgentStats.objects.filter(
            member=agent,
            year=current_year,
        ).first()

        # Get recent transactions (as listing agent)
        context["listing_transactions"] = Property.objects.filter(
            list_agent_key_numeric=agent.member_key_numeric,
            standard_status="Closed",
        ).order_by("-close_date")[:10]

        # Get recent transactions (as buyer agent)
        context["buyer_transactions"] = Property.objects.filter(
            buyer_agent_key_numeric=agent.member_key_numeric,
            standard_status="Closed",
        ).order_by("-close_date")[:10]

        # Calculate total stats
        all_transactions = Property.objects.filter(
            Q(list_agent_key_numeric=agent.member_key_numeric)
            | Q(buyer_agent_key_numeric=agent.member_key_numeric),
            standard_status="Closed",
            close_date__year=current_year,
        )
        context["year_transaction_count"] = all_transactions.count()
        context["year_volume"] = sum(
            Decimal(str(p.close_price or 0)) for p in all_transactions
        )

        return context


class PropertyListView(ListView):
    """Property search view with filtering."""

    model = Property
    template_name = "properties.html"
    context_object_name = "properties"
    paginate_by = 25

    def get_queryset(self) -> QuerySet[Property]:
        """Get filtered and ordered queryset.

        Returns:
            Filtered Property queryset.
        """
        queryset = Property.objects.order_by("-close_date", "-close_price")

        # Apply filters
        self.filterset = PropertyFilter(self.request.GET, queryset=queryset)
        return self.filterset.qs

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        """Get context data for the property list.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Context dictionary with filter and summary stats.
        """
        context = super().get_context_data(**kwargs)
        context["filterset"] = self.filterset
        context["total_properties"] = self.filterset.qs.count()

        # Calculate summary for filtered results
        filtered_qs = self.filterset.qs
        context["total_volume"] = filtered_qs.aggregate(
            total=Sum("close_price")
        )["total"] or Decimal("0")

        return context


class PropertyDetailView(DetailView):
    """Property detail view."""

    model = Property
    template_name = "property_detail.html"
    context_object_name = "property"
    slug_field = "listing_key_numeric"
    slug_url_kwarg = "listing_key"

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        """Get context data for the property detail.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Context dictionary with related agent info.
        """
        context = super().get_context_data(**kwargs)
        prop = self.object

        # Get listing agent details
        if prop.list_agent_key_numeric:
            context["list_agent"] = Member.objects.filter(
                member_key_numeric=prop.list_agent_key_numeric
            ).first()

        # Get buyer agent details
        if prop.buyer_agent_key_numeric:
            context["buyer_agent"] = Member.objects.filter(
                member_key_numeric=prop.buyer_agent_key_numeric
            ).first()

        return context

