# Development

## Development Pattern

Generally speaking, development uses git branches to manage progress on features and bugs while
maintaining a stable and versioned `main` branch while developing most features from the `develop`
branch as per the [git-flow model](https://nvie.com/posts/a-successful-git-branching-model/) and
product road-mapping as per [issues in milestones](https://github.com/BayAreaMetro/tm2py/milestones)
and managed in the [project board](https://github.com/BayAreaMetro/tm2py/projects).

```mermaid
    gitGraph
      commit  id: "a"
      branch develop
      checkout develop
      commit id: "initial development setup"
      branch featureA
      checkout featureA
      commit id: "initial try"
      commit id: "more work"
      commit id: "Passes Tests"
      checkout develop
      commit id: "small change"
      checkout featureA
      merge develop
      commit id: "Passes Tests w/Develop Updates"
      checkout develop
      merge featureA
      branch featureB
      checkout featureB
      commit id: "work on another feature"
      commit id: "b"
      checkout develop
      merge featureB
      checkout main
      merge develop
      branch release
      checkout release
      commit tag: "v0.9-prerelease"
      commit tag: "v0.9"
      checkout main
      merge release
      checkout develop
      merge main

```

## How to Contribute

The following are the general steps taken to contribute to `tm2py`.

### Issue Development

Generally-speaking, all contributions should support an issue which has a
clearly-defined user-story, a set of tests/conditions which need to be demonstrated in order to
close the issue, an agreed-upon approach, and is assigned to the person who should be working on it.

### Branch

Use [GitHub's branching](https://docs.github.com/en/get-started/quickstart/github-flow) capabilities
to create a feature branch from the main `develop` branch which is clearly named (e.g. features:`feat-add-transit-assignment` bug fixes: `fix-crash-macosx`) and check it out.  

=== "Terminal"

    ```sh
    git checkout develop
    git checkout -b fix-maxos-crash
    ```

=== "GitHub Desktop"

    [Managing branches documentation(https://docs.github.com/en/desktop/contributing-and-collaborating-using-github-desktop/making-changes-in-a-branch/managing-branches)]

### Develop tests

As much as possible, we use
[test-driven development](https://en.wikipedia.org/wiki/Test-driven_development) in order to clearly
define when the work is done and working.  This can be acheived through writing a new test or
extending another test.  **When this is complete, the specified test should fail.**

### Fix issue tests/Address user story

Complete development using the approach agreed upon in the issue.  **When this
is complete, the tests for the issue should pass and the user story in the issue should
be satisfied**

General notes about code style:

- Use PEP8 general style and Google-style docstrings
- Add logging statements throutout using the [logging module](#Logging)
- Clarity over concision  
- Expicit over implicit
- Add comments for non-obvious code where it would take a user a while to figure out

Confirm tests run:

=== "With Emme"

    If you have Emme installed, it will automatically run the tests with Emme environment.

    ```sh
    pytest -s
    ```

=== "Using Mocked Emme Environment"

    If you have Emme installed but want to force running the tests with the Mock:

    ```sh
    pytest --inro mock
    ```

### Update/address other failing tests

Update your branch with the most recent version of the develop
branch (which may have moved forward), resolving any merge-conflicts and other tests that may now
be failing. **When this is complete, all tests should pass.**

!!! tip

    You can (and should) push your changes throughout your work so that others can see what you
    are working on, contribute advice, and sometimes work on the issue with you.

### Update relevant documentation

See the [Docmentation on Documentation](./documentation.md).

### Tidy your work

In order to make sure all changes comply with our requirements and are consistent
with specifications (i.e. for markdown files, which aren't tested in `pytest`), we use
[`pre-commit`](https://pre-commit.com/):

```sh

pre-commit run --all-files
pre-commit run --hook-stage manual --all-files

```

!!! tip

    Often pre-commit checks will "fail" on the first run when they are fixing the issues.  When
    you run it again, hopefully it will be successful.

### Pull-Request

Create the pull-request which clearly defines what the pull request contains
and link it to the issues it addresses in the description via closing keywords (if applicable) or
references.  Finally, please assign reviewers who should review the pull-request prior to it being
merged and address their requested changes.

### Review and Agree on Pull Request with Reviewers

Pull request author should be responsive to reviewer questions and comments, addressing them in-line and through updated code pushes.

### Merge

Merge approved pull-request to `develop` using the `squash all changes` functionality
so that it appears as a single commit on the `develop` branch. Resolve any merge conflicts and
closing any issues which were fully addressed.

## Logging

The Logging module has the following levels:

- *display*  
- *file*  
- *fallback*  

In addition, there are:

- *override* logging level filter by component name and iteration, and  
- notify slack component (untested at this time)

### Logging Levels

Here are the log levels as defined in `TM2PY`:

| **Level** | **Description** |
| --------- | --------------- |
|TRACE| Highly detailed information which would rarely be of interest except for detailed debugging by a developer.|
|DEBUG| diagnostic information which would generally be useful to a developer debugging the model code; this may also be useful to a model operator in some cases.|
|DETAIL| more detail than would normally be of interest, but might be useful to a model operator debugging a model run / data or understanding model results.|
|INFO| messages which would normally be worth recording about the model operation.|
|STATUS| top-level, model is running type messages. There should be relatively few of these, generally one per component, or one per time period if the procedure is long.|
|WARN| warning messages where there is a possibility of a problem.|
|ERROR| problem causing operation to halt which is normal (or not unexpected) in scope, e.g. file does not exist. Includes general Python exceptions.|
|FATAL| severe problem requiring operation to stop immediately.

!!! Note

    In practice there may not be a large distinction between ERROR and FATAL in tm2py context.

### Adding log statements in code

Messages can be recorded using:

```python
logger.log(level="INFO")

#or equivalently

logger.info()
```

Additional arguments:
- Indent the message: `indent=True`

Group log messages together:
- Using a context: `logger.log_start_end()`
- Using a decorator:

```python
@LogStartEnd("Highway assignment and skims", level="STATUS")
def run(self):
```

### Viewing logging

Log messages can be shown in the console / notebook (using the logging.display_level)

```python
import logging
logging.display_level = "INFO" # or DEBUG, etc.
```

Log files with written log messages are split into:

=== "**Run Log**"
    For model overview.

    | **Settings** |                          |
    | ----------- | ------------------------ |
    | *Location:* | `logging.run_file_path`  |
    | *Level:*    | `logging.run_file_level` |

=== "**Debug Log**"
    A more detailed log.

    | **Settings** |                          |
    | ----------- | ------------------------ |
    | *Location:* | `logging.log_file_path`  |
    | *Level:*    | `logging.log_file_level` |

=== "**Catch-all Log**"
    Will output all log messages recorded.

    | **Settings** |                          |
    | ----------- | ------------------------ |
    | *Location:* | `logging.log_on_error_file_path`  |
    | *Level:*    | All... |

!!! Note

    Some logging can be conditional to only run if the log level is filtered in.

    e.g. if it takes a long time to generate the report. There is an example of this in the highway assignment which generates a report of the matrix results statistics only if DEBUG is filtered in for at least one of the log_levels.

### Additional Settings

#### Locally override logging level for debugging

The `logging.iter_component_level` can be used to locally override the logging level filter for debug purposes. This is specified as one or more tuples of (iteration, component_name, log_level).

!!! Example

    Record **all** messages during the highway component run at iteration 2:

    ```
    logging.iter_component_level: [ [2, "highway", "TRACE"] ]
    ```
