"""Django filters for MLS Sales Dashboard.

This module contains filter classes for filtering agents and properties
in list views.
"""

import django_filters
from django import forms
from django.db.models import QuerySet
from django.utils import timezone

from .models import AgentStats, Member, Property


class AgentStatsFilter(django_filters.FilterSet):
    """Filter for agent statistics/leaderboard.

    Allows filtering by AOR, year, and minimum volume.
    """

    aor = django_filters.CharFilter(
        field_name="aor",
        lookup_expr="iexact",
        label="Association of Realtors",
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    year = django_filters.NumberFilter(
        field_name="year",
        label="Year",
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    min_volume = django_filters.NumberFilter(
        field_name="total_volume",
        lookup_expr="gte",
        label="Minimum Volume ($)",
        widget=forms.NumberInput(attrs={"class": "form-control", "placeholder": "0"}),
    )
    min_transactions = django_filters.NumberFilter(
        field_name="transaction_count",
        lookup_expr="gte",
        label="Minimum Transactions",
        widget=forms.NumberInput(attrs={"class": "form-control", "placeholder": "0"}),
    )

    class Meta:
        """Meta options for AgentStatsFilter."""

        model = AgentStats
        fields = ["aor", "year"]

    def __init__(self, *args, **kwargs) -> None:
        """Initialize filter with dynamic choices.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        super().__init__(*args, **kwargs)

        # Populate AOR choices from existing data
        aors = (
            AgentStats.objects.values_list("aor", flat=True)
            .distinct()
            .order_by("aor")
        )
        aor_choices = [("", "All AORs")] + [(aor, aor) for aor in aors if aor]
        self.filters["aor"].extra["widget"] = forms.Select(
            choices=aor_choices,
            attrs={"class": "form-select"},
        )

        # Populate year choices
        years = (
            AgentStats.objects.values_list("year", flat=True)
            .distinct()
            .order_by("-year")
        )
        year_choices = [(year, str(year)) for year in years]
        if not year_choices:
            current_year = timezone.now().year
            year_choices = [(current_year, str(current_year))]
        self.filters["year"].extra["widget"] = forms.Select(
            choices=year_choices,
            attrs={"class": "form-select"},
        )


class MemberFilter(django_filters.FilterSet):
    """Filter for member (agent) search.

    Allows filtering by name, AOR, and office.
    """

    name = django_filters.CharFilter(
        method="filter_name",
        label="Agent Name",
        widget=forms.TextInput(
            attrs={"class": "form-control", "placeholder": "Search by name..."}
        ),
    )
    aor = django_filters.CharFilter(
        field_name="member_aor",
        lookup_expr="iexact",
        label="Association of Realtors",
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    office = django_filters.CharFilter(
        field_name="office_name",
        lookup_expr="icontains",
        label="Office Name",
        widget=forms.TextInput(
            attrs={"class": "form-control", "placeholder": "Search by office..."}
        ),
    )

    class Meta:
        """Meta options for MemberFilter."""

        model = Member
        fields = ["name", "aor", "office"]

    def __init__(self, *args, **kwargs) -> None:
        """Initialize filter with dynamic choices.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        super().__init__(*args, **kwargs)

        # Populate AOR choices
        aors = (
            Member.objects.values_list("member_aor", flat=True)
            .distinct()
            .order_by("member_aor")
        )
        aor_choices = [("", "All AORs")] + [(aor, aor) for aor in aors if aor]
        self.filters["aor"].extra["widget"] = forms.Select(
            choices=aor_choices,
            attrs={"class": "form-select"},
        )

    def filter_name(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        """Filter members by name (first, last, or full name).

        Args:
            queryset: The queryset to filter.
            name: The filter field name.
            value: The search value.

        Returns:
            Filtered queryset.
        """
        if not value:
            return queryset
        return queryset.filter(
            models.Q(member_full_name__icontains=value)
            | models.Q(member_first_name__icontains=value)
            | models.Q(member_last_name__icontains=value)
        )


class PropertyFilter(django_filters.FilterSet):
    """Filter for property search.

    Allows filtering by status, city, price range, date range, and agent.
    """

    status = django_filters.CharFilter(
        field_name="standard_status",
        lookup_expr="iexact",
        label="Status",
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    city = django_filters.CharFilter(
        field_name="city",
        lookup_expr="iexact",
        label="City",
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    property_type = django_filters.CharFilter(
        field_name="property_type",
        lookup_expr="iexact",
        label="Property Type",
        widget=forms.Select(attrs={"class": "form-select"}),
    )
    min_price = django_filters.NumberFilter(
        field_name="close_price",
        lookup_expr="gte",
        label="Min Price ($)",
        widget=forms.NumberInput(
            attrs={"class": "form-control", "placeholder": "Min price"}
        ),
    )
    max_price = django_filters.NumberFilter(
        field_name="close_price",
        lookup_expr="lte",
        label="Max Price ($)",
        widget=forms.NumberInput(
            attrs={"class": "form-control", "placeholder": "Max price"}
        ),
    )
    close_date_after = django_filters.DateFilter(
        field_name="close_date",
        lookup_expr="gte",
        label="Closed After",
        widget=forms.DateInput(attrs={"class": "form-control", "type": "date"}),
    )
    close_date_before = django_filters.DateFilter(
        field_name="close_date",
        lookup_expr="lte",
        label="Closed Before",
        widget=forms.DateInput(attrs={"class": "form-control", "type": "date"}),
    )
    agent = django_filters.CharFilter(
        method="filter_agent",
        label="Agent Name",
        widget=forms.TextInput(
            attrs={"class": "form-control", "placeholder": "Search by agent name..."}
        ),
    )
    address = django_filters.CharFilter(
        method="filter_address",
        label="Address",
        widget=forms.TextInput(
            attrs={"class": "form-control", "placeholder": "Search by address..."}
        ),
    )

    class Meta:
        """Meta options for PropertyFilter."""

        model = Property
        fields = [
            "status",
            "city",
            "property_type",
            "min_price",
            "max_price",
            "close_date_after",
            "close_date_before",
            "agent",
            "address",
        ]

    def __init__(self, *args, **kwargs) -> None:
        """Initialize filter with dynamic choices.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        super().__init__(*args, **kwargs)

        # Populate status choices
        statuses = (
            Property.objects.values_list("standard_status", flat=True)
            .distinct()
            .order_by("standard_status")
        )
        status_choices = [("", "All Statuses")] + [
            (s, s) for s in statuses if s
        ]
        self.filters["status"].extra["widget"] = forms.Select(
            choices=status_choices,
            attrs={"class": "form-select"},
        )

        # Populate city choices
        cities = (
            Property.objects.values_list("city", flat=True)
            .distinct()
            .order_by("city")
        )
        city_choices = [("", "All Cities")] + [(c, c) for c in cities if c]
        self.filters["city"].extra["widget"] = forms.Select(
            choices=city_choices,
            attrs={"class": "form-select"},
        )

        # Populate property type choices
        types = (
            Property.objects.values_list("property_type", flat=True)
            .distinct()
            .order_by("property_type")
        )
        type_choices = [("", "All Types")] + [(t, t) for t in types if t]
        self.filters["property_type"].extra["widget"] = forms.Select(
            choices=type_choices,
            attrs={"class": "form-select"},
        )

    def filter_agent(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        """Filter properties by agent name (list or buyer agent).

        Args:
            queryset: The queryset to filter.
            name: The filter field name.
            value: The search value.

        Returns:
            Filtered queryset.
        """
        if not value:
            return queryset
        from django.db.models import Q

        return queryset.filter(
            Q(list_agent_full_name__icontains=value)
            | Q(buyer_agent_full_name__icontains=value)
        )

    def filter_address(self, queryset: QuerySet, name: str, value: str) -> QuerySet:
        """Filter properties by address.

        Args:
            queryset: The queryset to filter.
            name: The filter field name.
            value: The search value.

        Returns:
            Filtered queryset.
        """
        if not value:
            return queryset
        from django.db.models import Q

        return queryset.filter(
            Q(unparsed_address__icontains=value)
            | Q(street_name__icontains=value)
            | Q(street_number__icontains=value)
        )

