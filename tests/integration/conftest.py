#!/usr/bin/env python3
# Copyright 2023-2025 Canonical Ltd.
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

"""Configure Slurm charm integration tests."""

import logging
from collections.abc import Iterator

import pytest
from aiosmtpd.controller import Controller
from bdd_utils import MailHandler, interface_ipv4
from constants import NETWORK_INTERFACE, SMTP_SERVER_PORT
from pytest_bdd import parsers, when
from pytest_jubilant_bdd import Context

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def base(request: pytest.FixtureRequest) -> str:
    """Get the base to deploy the Slurm charms on."""
    return request.config.getoption("--charm-base")


@pytest.fixture
def scenario_state() -> dict:
    """Per-scenario mutable state shared between Given/When/Then steps."""
    return {}


@pytest.fixture(scope="module")
def smtp_handler() -> Iterator[MailHandler]:
    """Start a local SMTP server capturing Slurm notification emails."""
    handler = MailHandler()
    ip_address = interface_ipv4(NETWORK_INTERFACE)
    controller = Controller(handler, hostname=ip_address, port=SMTP_SERVER_PORT)
    controller.start()
    try:
        yield handler
    finally:
        controller.stop()


@when(parsers.parse("I reset the node configuration on unit '{unit}'"))
def reset_node_config(context: Context, unit: str) -> None:
    """Reset the slurmd node configuration via the ``set-node-config`` action.

    Custom step that passes ``reset`` as a proper Python ``bool``. The built-in
    ``run_action`` step uses ``make_dict`` which autocasts via
    ``ast.literal_eval`` and cannot parse lowercase ``true`` into a Python bool,
    so the slurmd action rejects it.
    """
    juju = context.get_juju()
    juju.run(unit, "set-node-config", params={"reset": True})


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--charm-base",
        action="store",
        default="ubuntu@24.04",
        help="the base to deploy the slurm charms on during the integration tests",
    )
    parser.addoption(
        "--keep-models",
        action="store_true",
        default=False,
        help="keep temporarily created models",
    )
    parser.addoption(
        "--run-high-availability",
        action="store_true",
        default=False,
        help="run high availability tests (slow)",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "high_availability: marks tests for slurmctld high availability"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-high-availability"):
        # Flag given in cli: do not skip tests
        return
    skip_ha = pytest.mark.skip(reason="need --run-high-availability option to run")
    for item in items:
        if "high_availability" in item.keywords:
            item.add_marker(skip_ha)
