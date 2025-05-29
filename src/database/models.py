import uuid
from peewee import (
    Model, CharField, IntegerField, FloatField, DateField, UUIDField, ForeignKeyField
)
from database.db import db
from config.settings import settings


class BaseModel(Model):
    """Base model for database connection."""
    class Meta:
        database = db
        schema = settings.psql_schema

class ProjectModel(BaseModel):
    """Model for storing project information to separate and aggregate data."""
    id = UUIDField(primary_key=True, default=uuid.uuid4)  # Unique identifier
    status = CharField(max_length=10, null=True)  # new, active, close
    project_name = CharField(max_length=100)  # Factory or brand segment
    currency = CharField(max_length=3, default="USD")  # Project currency (RUB, USD, CNY, EUR)
    description = CharField(max_length=255, null=True)  # Optional project description

    class Meta:
        table_name = "projects"
        indexes = ((("project_name", "currency"), True),)

class SalesModel(BaseModel):
    """Model for storing sales data (GS, GM, EWC) by segment and month."""
    id = UUIDField(primary_key=True, default=uuid.uuid4)  # Unique identifier
    project = ForeignKeyField(ProjectModel, backref='sales')  # Reference to project
    segment = CharField(max_length=50)  # B2B, B2C for GM calculation
    date_of_month_begin = DateField()  # Start date of the month
    total_gs = FloatField(null=True, default=0)  # Gross Sales in USD
    total_ewc = FloatField(null=True, default=0)  # Estimated Weighted Cost in USD
    total_gm = FloatField(null=True, default=0)  # Gross Margin in USD

    class Meta:
        table_name = "sales"
        indexes = ((("project_id", "date_of_month_begin", "segment"), True),)

class OrdersModel(BaseModel):
    """Model for storing order information."""
    id = UUIDField(primary_key=True, default=uuid.uuid4)  # Unique identifier
    project = ForeignKeyField(ProjectModel, backref='orders')  # Reference to project
    order_date = DateField()  # Date when the order was placed
    production_time = IntegerField(null=True, default=0)  # Production lead time in days
    enroute_time = IntegerField(null=True, default=0)  # Logistics delivery time in days
    order_cost = FloatField(null=True, default=0)  # Order cost in project currency

    class Meta:
        table_name = "orders"
        indexes = ((("project_id", "order_date"), True),)

class LogisticsPaymentsTermsModel(BaseModel):
    """Model for storing logistics payment terms and EWC coefficient."""
    id = UUIDField(primary_key=True, default=uuid.uuid4)  # Unique identifier
    project = ForeignKeyField(ProjectModel, backref='logistics')  # Reference to project
    date_of_month_begin = DateField()  # Start date of the month
    delivery_duty_coef = FloatField(null=True, default=1.00)  # Logistics coefficient (e.g., 1.4 for EWC calculation)

    class Meta:
        table_name = "logistics"
        indexes = ((("project_id", "date_of_month_begin"), True),)

class OrdersPaymentsTermsModel(BaseModel):
    """Model for storing order payment terms."""
    id = UUIDField(primary_key=True, default=uuid.uuid4)  # Unique identifier
    project = ForeignKeyField(ProjectModel, backref='orders_payments')  # Reference to project
    date_of_month_begin = DateField()  # Start date of the month
    order_placement_date_payment_prc = FloatField(null=True, default=30.00)  # Prepayment percentage (e.g., 30%)
    order_shipment_date_payment_prc = FloatField(null=True, default=70.00)  # Payment before shipment percentage (e.g., 70%)

    class Meta:
        table_name = "orders_payment_terms"
        indexes = ((("project_id", "date_of_month_begin"), True),)

class PaymentsModel(BaseModel):
    """Model for storing payment information."""
    id = UUIDField(primary_key=True, default=uuid.uuid4)  # Unique identifier
    project = ForeignKeyField(ProjectModel, backref='payments')  # Reference to project
    date_of_month_begin = DateField()  # Start date of the month
    order_payment_total = FloatField(null=True, default=0)  # Payments for orders (prepayment + shipment) in project currency
    delivery_duty_payment_total = FloatField(null=True, default=0)  # Payments for delivery and customs duties in project currency

    class Meta:
        table_name = "payments"
        indexes = ((("project_id", "date_of_month_begin"), True),)

class StockBudgetModel(BaseModel):
    """Model for storing stock budget in EWC and cost prices in project currency."""
    id = UUIDField(primary_key=True, default=uuid.uuid4)  # Unique identifier
    project = ForeignKeyField(ProjectModel, backref='stock_budgets')  # Reference to project
    date_of_month_begin = DateField()  # Start date of the month
    oh_ewc_begin = FloatField(null=True, default=0)  # Stock at period start (EWC)
    oh_ewc_incoming = FloatField(null=True, default=0)  # Incoming stock (EWC)
    oh_ewc_outgoing = FloatField(null=True, default=0)  # Outgoing stock (EWC)
    oh_ewc_end = FloatField(null=True, default=0)  # Stock at period end (EWC)

    class Meta:
        table_name = "stock_budget"
        indexes = ((("project_id", "date_of_month_begin"), True),)