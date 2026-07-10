# Copyright 2026 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""BDD step definitions for Slurm mail notifications.

Covers SMTP integrator deployment with host/port config, job notification
emails (end, fail, begin with custom signature), and SMTP integrator
removal. Uses a module-scoped local SMTP server to capture emails.
"""

import logging

import jubilant
import pytest
from bdd_utils import MailHandler, interface_ipv4
from constants import SLURMD_APP_NAME, SMTP_INTEGRATOR_APP_NAME
from pytest_bdd import given, parsers, scenarios, then, when
from pytest_jubilant_bdd import Context

logger = logging.getLogger(__name__)

# Order 14: after job submission (12) and oci runtime (13), before HA (19).
pytestmark = pytest.mark.order(14)

scenarios("features/slurm_mail_notifications.feature")


# ---------------------------------------------------------------------------
# SMTP integrator deploy / removal
# ---------------------------------------------------------------------------


@given(parsers.parse("I deploy 'smtp-integrator' with host and port '{port}'"))
def deploy_smtp_integrator(context: Context, port: str) -> None:
    """Deploy ``smtp-integrator`` with the local interface IP and given port."""
    juju = context.get_juju()
    from constants import NETWORK_INTERFACE

    juju.deploy(
        "smtp-integrator",
        SMTP_INTEGRATOR_APP_NAME,
        config={"host": interface_ipv4(NETWORK_INTERFACE), "port": int(port)},
    )


@when(parsers.parse("I remove application '{app}'"))
def remove_application(context: Context, app: str) -> None:
    """Remove an application and wait for it to disappear from the model."""
    juju = context.get_juju()
    juju.remove_application(app)
    juju.wait(lambda status: app not in status.apps)


# ---------------------------------------------------------------------------
# Slurm job submission with mail notifications
# ---------------------------------------------------------------------------


@when(
    parsers.parse(
        "I run a slurm job on unit '{unit}' with mail user '{to_address}' "
        "and mail type '{mail_type}'"
    )
)
def run_slurm_job_mail(context: Context, unit: str, to_address: str, mail_type: str) -> None:
    """Run a successful srun job that triggers a Slurm mail notification."""
    juju = context.get_juju()
    juju.exec(
        f"srun --time=1 --partition {SLURMD_APP_NAME} "
        f"--mail-user={to_address} --mail-type={mail_type} sleep 1",
        unit=unit,
    )


@when(
    parsers.parse(
        "I run a failing slurm job on unit '{unit}' with mail user '{to_address}' "
        "and mail type '{mail_type}'"
    )
)
def run_failing_slurm_job_mail(
    context: Context, unit: str, to_address: str, mail_type: str
) -> None:
    """Run a failing srun job that triggers a Slurm failure mail notification."""
    juju = context.get_juju()
    try:
        juju.exec(
            f"srun --time=1 --partition {SLURMD_APP_NAME} "
            f"--mail-user={to_address} --mail-type={mail_type} "
            f"bash -c 'sleep 1; exit 1'",
            unit=unit,
        )
    except jubilant.TaskError:
        # Failure is intentional — it triggers the FAIL notification email.
        pass


# ---------------------------------------------------------------------------
# Email verification
# ---------------------------------------------------------------------------


@then(
    parsers.parse(
        "an email is received by '{to_address}' with subject matching '{subject_pattern}' "
        "and content matching '{content_pattern}'"
    )
)
def email_received(
    context: Context,
    smtp_handler: MailHandler,
    to_address: str,
    subject_pattern: str,
    content_pattern: str,
) -> None:
    """Poll the SMTP handler until an email matching the patterns is received."""

    def ready(_ctx: Context) -> bool:
        try:
            smtp_handler.assert_mail(to_address, subject_pattern, content_pattern)
            return True
        except AssertionError:
            return False

    context.wait(ready=ready)
