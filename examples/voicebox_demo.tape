# This demo requires a VOICEBOX_API_TOKEN environment variable.
# The example uses anonymous user and express endpoint by default.
# The app token should be for a Voicebox app configured to use:
# - Anonymous user authentication against Stardog Express server  
# - The c360-kit dataset (questions can be adapted for your dataset)
# You may need additional env vars (STARDOG_ENDPOINT, STARDOG_USERNAME, etc.)
# Get your app token from the Stardog Cloud console.
#
# To record this demo, from the root of this repo, run:
# vhs examples/voicebox_demo.tape
#
# Note: a virtual environment is expected to be set up in the `venv` directory
# with the required dependencies installed.

Output examples/voicebox_demo.gif

# VHS Settings
Set FontSize 16
Set Width 1200
Set Height 700
Set Margin 20
Set MarginFill "#674EFF"
Set BorderRadius 10
Set PlaybackSpeed 1.5


# Setup
Hide
Type "source venv/bin/activate && clear"
Enter
Show

# Start the example
Type "python examples/voicebox_example.py"
Enter
Sleep 3s

Type "How many products are there?"
Enter
Sleep 6s

Type "What's the most expensive one? Tell me about it."
Enter
Sleep 6s

Type "quit"
Enter
Sleep 3s
