"""
Interactive Voicebox example demonstrating the two-step query process.

This example shows how to:
1. Use Voicebox to ask natural language questions against your knowledge graph
2. Use pystardog to execute the generated SPARQL query using database settings 
   from your Voicebox app configuration

Key concepts demonstrated:
- Voicebox apps have pre-configured database settings managed in Stardog Cloud
- When using pystardog directly, you must provide your own connection credentials
- The same database and graph settings are used for both steps

Requirements:
- pip install pystardog[cloud] rich
- Valid Voicebox API token from Stardog Cloud console
- Stardog connection credentials for query execution
"""

import argparse
import os
from dataclasses import dataclass
from typing import Optional

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.syntax import Syntax
from rich.table import Table

import stardog
import stardog.cloud
import stardog.cloud.exceptions
import stardog.exceptions
from stardog.cloud.voicebox import VoiceboxApp, VoiceboxAppSettings


@dataclass
class VoiceboxConfig:
    """Configuration for Voicebox Cloud API access."""

    app_api_token: str
    client_id: str


@dataclass
class StardogConfig:
    """Direct Stardog connection configuration."""

    endpoint: str
    username: str
    password: str


@dataclass
class AppConfig:
    """Complete application configuration."""

    voicebox: VoiceboxConfig
    stardog: StardogConfig


def get_configuration(args: argparse.Namespace) -> AppConfig:
    """Get configuration from CLI args with environment variable fallbacks."""
    # CLI args take precedence over environment variables
    app_api_token = args.app_api_token or os.getenv("VOICEBOX_API_TOKEN")
    client_id = args.client_id or os.getenv("VOICEBOX_CLIENT_ID") or "pystardog-example"

    if not app_api_token:
        raise ValueError(
            "VOICEBOX_API_TOKEN is required. "
            "Use --app-api-token CLI arg or set VOICEBOX_API_TOKEN environment variable. "
            "The API token is obtained from your Stardog Cloud console."
        )

    voicebox_config = VoiceboxConfig(app_api_token=app_api_token, client_id=client_id)

    # For Stardog config, CLI args provide defaults, env vars can override
    stardog_config = StardogConfig(
        endpoint=os.getenv("STARDOG_ENDPOINT", args.stardog_endpoint),
        username=os.getenv("STARDOG_USERNAME", args.stardog_username),
        password=os.getenv("STARDOG_PASSWORD", args.stardog_password),
    )

    return AppConfig(voicebox=voicebox_config, stardog=stardog_config)


def display_app_settings(
    voicebox: VoiceboxApp, console: Console
) -> Optional[VoiceboxAppSettings]:
    """Display Voicebox app configuration and return settings."""
    try:
        settings = voicebox.settings()
        settings_text = f"[magenta]Name:[/magenta] {settings.name}\n"
        settings_text += f"[magenta]Database:[/magenta] {settings.database}\n"
        settings_text += f"[magenta]Model:[/magenta] {settings.model}\n"
        settings_text += (
            f"[magenta]Named Graphs:[/magenta] {', '.join(settings.named_graphs)}\n"
        )
        settings_text += f"[magenta]Reasoning:[/magenta] {'Enabled' if settings.reasoning else 'Disabled'}"

        console.print(
            Panel.fit(
                settings_text,
                title="Voicebox App Configuration",
                border_style="magenta",
            )
        )
        return settings
    except stardog.cloud.exceptions.StardogCloudException as e:
        error_type = type(e).__name__
        console.print(f"[red]Stardog Cloud Error ({error_type}): {e}[/red]")
        return None
    except Exception as e:
        console.print(f"[red]Error: Could not retrieve app settings: {e}[/red]")
        return None


def execute_sparql_query(
    sparql_query: str,
    database_name: str,
    named_graphs: list,
    stardog_config: StardogConfig,
    console: Console,
) -> None:
    """Execute SPARQL query using direct Stardog connection."""
    try:
        with console.status("[bold blue]Executing query...", spinner="dots"):
            with stardog.Connection(
                database_name,
                endpoint=stardog_config.endpoint,
                username=stardog_config.username,
                password=stardog_config.password,
            ) as conn:
                results = conn.select(
                    sparql_query,
                    default_graph_uri=named_graphs,
                )
                if results:
                    results = stardog.SelectQueryResult(results)

        display_query_results(results, console)

    except stardog.exceptions.StardogException as e:
        console.print(f"[red]Query execution failed:[/red]")
        console.print(f"[red]  {e}[/red]")
        console.print(
            "[dim]  💡 Check your Stardog connection settings (CLI args or environment variables)[/dim]"
        )
    except Exception as e:
        console.print(f"[red]Error executing query:[/red]")
        console.print(f"[red]  {e}[/red]")


def display_query_results(results, console: Console) -> None:
    """Display SPARQL query results in a formatted table."""
    if results:
        console.print("\n[bold blue]Query Results:[/bold blue]")

        table = Table(show_header=True, header_style="bold blue")
        first_result = results[0]
        for var in first_result.keys():
            table.add_column(var)

        for result in results[:10]:  # Limit to first 10 results
            row = [str(result.get(var, "")) for var in first_result.keys()]
            table.add_row(*row)

        console.print(table)

        if len(results) > 10:
            console.print(f"[dim]... and {len(results) - 10} more results[/dim]")
    else:
        console.print("[dim]No results returned[/dim]")


def run_interactive_session(
    voicebox: VoiceboxApp,
    settings: VoiceboxAppSettings,
    config: AppConfig,
    conversation_id: Optional[str],
    console: Console,
) -> None:
    """Run the main interactive question-answer loop."""
    if conversation_id:
        console.print(f"[dim]Continuing conversation: {conversation_id}[/dim]")
    else:
        console.print("[dim]Starting new conversation[/dim]")

    console.print("\n[dim]Type 'quit' to exit or press Ctrl+C[/dim]\n")

    while True:
        try:
            question = Prompt.ask("[bold cyan]Enter your question[/bold cyan]").strip()

            if question.lower() in ["quit", "exit", "q"]:
                console.print("[yellow]Goodbye![/yellow]")
                break

            if not question:
                continue

            # Ask Voicebox
            with console.status("[bold green]Asking Voicebox...", spinner="dots"):
                answer = voicebox.ask(question, conversation_id=conversation_id)

                if not conversation_id and answer.conversation_id:
                    console.print(
                        f"[dim]Conversation ID: {answer.conversation_id}[/dim]"
                    )

                conversation_id = answer.conversation_id

            # Display answer
            console.print(f"\n[bold green]Answer:[/bold green] ", end="")
            console.print(answer.content, highlight=False)

            if answer.interpreted_question:
                console.print(
                    f"[dim]Interpreted as:[/dim] {answer.interpreted_question}"
                )

            # Display and execute SPARQL query if available
            if answer.sparql_query:
                sparql_syntax = Syntax(
                    answer.sparql_query, "sparql", theme="monokai", line_numbers=True
                )
                console.print("\n[bold yellow]Generated SPARQL:[/bold yellow]")
                console.print(sparql_syntax)

                # Execute using direct Stardog connection (requires your configuration)
                execute_sparql_query(
                    answer.sparql_query,
                    settings.database,
                    settings.named_graphs,
                    config.stardog,
                    console,
                )

            console.print()

        except KeyboardInterrupt:
            console.print("\n[yellow]Goodbye![/yellow]")
            break
        except stardog.cloud.exceptions.StardogCloudException as e:
            error_type = type(e).__name__
            console.print(f"[red]Stardog Cloud Error ({error_type}): {e}[/red]")
        except Exception as e:
            console.print(f"[red]Unexpected Error: {e}[/red]")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Interactive Voicebox example that demonstrates the two-step process: "
        "1) Use Voicebox to ask questions against your knowledge graph, "
        "2) Use pystardog to execute the generated SPARQL query using the database settings from your Voicebox app configuration."
    )
    parser.add_argument(
        "--conversation-id",
        help="Continue an existing conversation (if not provided, starts a new one)",
    )

    # Voicebox configuration
    parser.add_argument(
        "--app-api-token",
        help="Voicebox API token (or set VOICEBOX_API_TOKEN env var)",
    )
    parser.add_argument(
        "--client-id",
        help="Voicebox client ID to identify the user (or set VOICEBOX_CLIENT_ID env var, default: pystardog-example)",
    )

    # Stardog configuration
    parser.add_argument(
        "--stardog-endpoint",
        default="https://express.stardog.cloud:5820",
        help="Stardog server endpoint (or set STARDOG_ENDPOINT env var, default: %(default)s)",
    )
    parser.add_argument(
        "--stardog-username",
        default="anonymous",
        help="Stardog username (or set STARDOG_USERNAME env var, default: %(default)s)",
    )
    parser.add_argument(
        "--stardog-password",
        default="anonymous",
        help="Stardog password (or set STARDOG_PASSWORD env var, default: %(default)s)",
    )

    args = parser.parse_args()

    console = Console()

    # Get configuration from CLI args or environment
    try:
        config = get_configuration(args)
    except ValueError as e:
        console.print(f"[red]Configuration Error: {e}[/red]")
        return

    console.print(
        Panel.fit(
            "🤖 [bold blue]Stardog Voicebox Interactive Example[/bold blue]",
            border_style="blue",
        )
    )

    # Connect to Stardog Cloud and initialize Voicebox app
    with stardog.cloud.Client() as client:
        voicebox = client.voicebox_app(
            app_api_token=config.voicebox.app_api_token,
            client_id=config.voicebox.client_id,
        )

        # Display app configuration and get settings
        settings = display_app_settings(voicebox, console)
        if not settings:
            return

        # Run interactive session
        run_interactive_session(
            voicebox, settings, config, args.conversation_id, console
        )


if __name__ == "__main__":
    main()
