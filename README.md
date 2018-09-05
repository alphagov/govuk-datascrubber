# GOV.UK Data Scrubber

This tool is intended to run in GOV.UK production environments in AWS to create
copies of backend databases with sensitive data removed. Such copies can then be
imported into development environments.

## Supported databases

* MySQL:
  * `whitehall`
* Postgres:
  * `email-alert-api`
  * `publishing_api`

# Mechanism of operation

RDS Database Snapshots are the basis for all the operations of the scrubber.
This means snapshots stored within the RDS service, and not in S3. See the [RDS
docs](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithAutomatedBackups.html)
for more details on snapshots.

Given an RDS snapshot, either MySQL or Postgres, the scrubber:

 * Spins up a temporary RDS DB Instance from that snapshot
 * Connects to it
 * Discovers the databases inside
 * Matches the discovered list against the internal list of supported databases
 * Executes database-specific code to scrub any databases matched
 * Shuts down the temporary instance, creating a "final snapshot"
 * Optionally shares the final snapshot with other AWS accounts

If any errors are encountered during the scrub process, the final snapshot
creation and sharing is aborted, so as to guard against accidental leakage of
sensitive data.

The RDS snapshot to start from can be specified in one of three ways:

 * Explicitly, with its RDS snapshot identifier
 * Implicitly, by taking the latest available automated snapshot of a given RDS
   database identifier
 * Implicitly, by taking the latest available automated shapshot of a given RDS
   database endpoint

Automated snapshots are daily backups that are taken from all DB instances by
default by RDS.

## Parallel operation

Spinning up RDS instances, running the SQL, creating the final snapshot, and
destroying the instances are all fairly time-consuming operations, although they
are not CPU intensive (at least, not on the machine running the scrubber).

Where multiple instances are required, e.g. one for MySQL and one for Postgres,
a worker thread is spawned per instance.

# Usage

To run parallel scrub tasks against `mysql-primary` and `postgresql-primary`:

  `$ datascrubber --mysql-hosts mysql-primary --postgresql-hosts postgresql-primary`

Sharing scrubbed snapshots with another AWS account:

  `$ datascrubber --mysql-hosts mysql-primary --share-with 123456789`

Logging to syslog:

  `$ datascrubber --mysql-hosts mysql-primary --log-mode syslog`

Note that if STDOUT is not a TTY (e.g. if being run by Cron) logging to syslog
is the default behaviour.

Increasing the log level:

  `$ datascrubber --mysql-hosts mysql-primary --log-level DEBUG`

This will increase the verbosity of Boto3 logging too.

# Prerequisites, development, deployment

The dependencies are Python 3 and the libraries listed in the
`requirements.txt`.

Docker is required to build the Debian package.

At run time, AWS credentials are required. The Boto3 library is used for all AWS
interactions, and AWS credentials are discovered in exactly the same manner as
the AWS CLI.

## virtualenv

Use [virtualenv](https://virtualenv.pypa.io/en/stable/) to work on the code locally:

```
virtualenv --python=python3 .env
source .env/bin/activate
pip3 install -r requirements.txt
```

## Supporting new databases

Database scrub tasks are defined in the `datascrubber/tasks/` directory. See
existing code for examples of each step:

 * Create a new Python script in that directory, containing a function
   that takes a database cursor as an argument.
 * Add an `import` statement for it into `datascrubber/tasks/__init__.py`.
 * Add the function to the `self.scrub_functions` dict in either the MySQL or
   or the Postgres task manager class (see `datascrubber/task_managers/`.

## Build process

To build the Debian package:

  `$ make deb`

