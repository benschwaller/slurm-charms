#!/usr/bin/env python3
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

"""Unit tests for the `slurmctld` charmed operators `config` module."""

import pytest
from config import ConfigData
from pydantic import ValidationError

EXAMPLE_CLUSTER_NAME = "charmed-hpc"
EXAMPLE_DEFAULT_PARTITION = "normal"
EXAMPLE_EMAIL_FROM_NAME = "Slurm Admin"


def make_config(**overrides) -> ConfigData:
    """Build a `ConfigData` instance, overriding only the options under test."""
    return ConfigData(
        **{
            "cgroup_parameters": "",
            "cluster_name": EXAMPLE_CLUSTER_NAME,
            "default_partition": EXAMPLE_DEFAULT_PARTITION,
            "email_from_name": EXAMPLE_EMAIL_FROM_NAME,
            "slurm_conf_parameters": "",
            **overrides,
        }
    )


class TestConfigData:
    """Unit tests for the `ConfigData` class."""

    @pytest.mark.parametrize(
        "slurm_conf_parameters",
        (
            pytest.param(
                "SlurmctldParameters=enable_configless=False",
                id="override disables configless",
            ),
            pytest.param(
                "SlurmctldParameters=idle_on_node_suspend=True",
                id="override sets other parameters",
            ),
        ),
    )
    def test_enable_configless_always_forced_true(self, slurm_conf_parameters: str) -> None:
        """`enable_configless` is always enabled, regardless of the operator override.

        Failure mode: an override either disables `enable_configless` outright or
        replaces the charm-managed `SlurmctldParameters` set without mentioning it.
        Either way configless mode is lost, compute nodes can no longer fetch
        `slurm.conf` from the controller, and the charm source documents this as
        cluster corruption.
        """
        config = make_config(slurm_conf_parameters=slurm_conf_parameters)

        assert config.slurm_conf_parameters.slurmctld_parameters["enable_configless"] is True

    def test_enable_configless_preserves_other_parameters(self) -> None:
        """Injecting `enable_configless` keeps the operator's other parameters intact.

        Failure mode: the charm clobbers the whole `SlurmctldParameters` set while
        forcing `enable_configless`, silently dropping operator configuration.
        """
        config = make_config(slurm_conf_parameters="SlurmctldParameters=idle_on_node_suspend=True")

        parameters = config.slurm_conf_parameters.slurmctld_parameters
        assert parameters["idle_on_node_suspend"] is True
        assert parameters["enable_configless"] is True

    def test_cgroup_override_defaults_are_applied(self) -> None:
        """The charm's default cgroup constraints are applied on top of the override.

        Failure mode: an override that only sets a subset of cgroup options
        wipes the required defaults (`ConstrainCores`, `ConstrainRAMSpace`,
        etc.), leaving `cgroup.conf` under-constrained.
        """
        config = make_config(cgroup_parameters="ConstrainDevices=yes")

        assert config.cgroup_parameters.constrain_devices is True
        assert config.cgroup_parameters.constrain_cores is True
        assert config.cgroup_parameters.constrain_ram_space is True
        assert config.cgroup_parameters.constrain_swap_space is True
        assert config.cgroup_parameters.signal_children_processes is True

    @pytest.mark.parametrize(
        "overrides,expected",
        (
            pytest.param(
                {"cgroup_parameters": "ThisIsNotAValidKey=yes"},
                "Invalid cgroup configuration",
                id="invalid cgroup override",
            ),
            pytest.param(
                {"slurm_conf_parameters": "NotARealSlurmDirective=5"},
                "Invalid slurm configuration override",
                id="invalid slurm.conf override",
            ),
        ),
    )
    def test_load_invalid_config(self, overrides: dict, expected: str) -> None:
        """A malformed configuration override is rejected, not applied.

        Failure mode: a typo'd directive is accepted and written to `cgroup.conf`
        or the overrides include file, preventing `slurmctld` from starting on the
        next service restart.
        """
        with pytest.raises(ValidationError, match=expected):
            make_config(**overrides)
