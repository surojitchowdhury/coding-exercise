# Hello C#

## Prerequisites

### Setting Up the Development Environment

You have two options to set up your development environment:

#### Option 1: Installing .NET Locally

1. Visit the official .NET download page: https://dotnet.microsoft.com/download

2. Download and install the latest version of the .NET SDK for your operating system (Windows, macOS, or Linux).

3. After installation, verify that .NET is correctly installed by opening a terminal or command prompt and running:

   ```shell
   dotnet --version
   ```

   This should display the version number of the installed .NET SDK.

4. If you encounter any issues, refer to the official .NET documentation for troubleshooting: https://docs.microsoft.com/en-us/dotnet/

#### Option 2: Using Dev Containers (Recommended)

If you're using Visual Studio Code or another IDE that supports Dev Containers, you can use the included `.devcontainer` files to set up a consistent development environment:

1. Ensure you have Docker installed on your system.

2. Install the "Remote - Containers" extension in VS Code.

3. Open the project folder in VS Code.

4. When prompted, click "Reopen in Container" or use the command palette (F1) and select "Remote-Containers: Reopen in Container".

5. VS Code will build the Docker container and set up the development environment with all necessary dependencies.

6. Once the container is built, you can use the integrated terminal in VS Code to run commands like `dotnet build` and `dotnet run`.

Using Dev Containers ensures that all developers work with the same environment, regardless of their local setup, and makes it easier to get started with the project quickly.

## Getting Started

Build the project:
   ```shell
   dotnet build
   ```

## Running the Project

To run the main project, use the following command in the terminal:

```shell
dotnet run --project HelloProject
```

## Running Tests

To run the tests for this project, use the following command:

```shell
dotnet test
```

This will discover and run all tests in the solution.
