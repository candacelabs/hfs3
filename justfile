# hfs3 — Mirror HuggingFace to S3

# Dev commands
test *args='':
    uv run pytest {{args}}

test-v *args='':
    uv run pytest -v {{args}}

install:
    uv sync --group dev

# Main workflows
mirror repo:
    uv run hfs3 mirror "{{repo}}"

pull repo dest='./repo':
    uv run hfs3 pull "{{repo}}" --dest "{{dest}}"

run repo dest='./repo' port='7860':
    uv run hfs3 run "{{repo}}" --dest "{{dest}}" --port "{{port}}"
