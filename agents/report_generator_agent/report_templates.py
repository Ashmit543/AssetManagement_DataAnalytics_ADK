# agents/report_generator_agent/report_templates.py

REPORT_TEMPLATES = {
    "Executive Summary": """
    Generate an executive summary for {ticker} based on the following financial data.
    Focus on key performance indicators, recent trends, and a concise outlook.

    Financial Data:
    {financial_data}

    Additional Parameters:
    {user_parameters}

    Provide a professional, concise summary suitable for an executive.
    """,

    "Market Overview": """
    Generate a concise market overview report based on general market trends.
    This report does not focus on a single company but on broader market conditions.

    Context/Focus:
    {user_parameters}

    Summarize recent market movements, economic indicators, and potential implications for investors.
    """
    # Add more report types and their corresponding templates here
    # e.g., "Sector Analysis", "Investment Thesis", etc.
}

def get_report_template(report_type: str) -> str:
    """
    Retrieves the appropriate report template based on the report type.
    """
    return REPORT_TEMPLATES.get(report_type)