import argparse
import os
from cwltool.software_requirements import SOFTWARE_REQUIREMENTS_ENABLED
from cwltool.utils import DEFAULT_TMP_PREFIX
from typing import (Any, AnyStr, Dict, List,  # pylint: disable=unused-import
                    Optional, Sequence, Text, Union, cast)
from cwltool.resolver import ga4gh_tool_registries

def arg_parser():  # type: () -> argparse.ArgumentParser
    parser = argparse.ArgumentParser(
        description='Runs a ceci pipeline from a configuration file.')
    parser.add_argument("--basedir", type=Text)
    parser.add_argument("--outdir", type=Text, default=os.path.abspath('.'),
                        help="Output directory, default current directory")

    parser.add_argument("--parallel", action="store_true", default=False,
                        help="[experimental] Run jobs in parallel. ")
    envgroup = parser.add_mutually_exclusive_group()
    envgroup.add_argument("--preserve-environment", type=Text, action="append",
                          help="Preserve specific environment variable when "
                          "running CommandLineTools.  May be provided multiple "
                          "times.", metavar="ENVVAR", default=["PATH"],
                          dest="preserve_environment")
    envgroup.add_argument("--preserve-entire-environment", action="store_true",
                          help="Preserve all environment variable when running "
                          "CommandLineTools.", default=False,
                          dest="preserve_entire_environment")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument("--rm-container", action="store_true", default=True,
                         help="Delete Docker container used by jobs after they exit (default)",
                         dest="rm_container")

    exgroup.add_argument(
        "--leave-container", action="store_false", default=True,
        help="Do not delete Docker container used by jobs after they exit",
        dest="rm_container")

    cidgroup = parser.add_argument_group(
        "Options for recording the Docker container identifier into a file.")
    # Disabled as containerid is now saved by default
    cidgroup.add_argument("--record-container-id", action="store_true",
                          default=False,
                          help = argparse.SUPPRESS,
                          dest="record_container_id")

    cidgroup.add_argument(
        "--cidfile-dir", type=Text, help="Store the Docker "
        "container ID into a file in the specified directory.",
        default=None, dest="cidfile_dir")

    cidgroup.add_argument(
        "--cidfile-prefix", type=Text,
        help="Specify a prefix to the container ID filename. "
        "Final file name will be followed by a timestamp. "
        "The default is no prefix.",
        default=None, dest="cidfile_prefix")

    parser.add_argument("--tmpdir-prefix", type=Text,
                        help="Path prefix for temporary directories",
                        default=DEFAULT_TMP_PREFIX)

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument("--tmp-outdir-prefix", type=Text,
                         help="Path prefix for intermediate output directories",
                         default=DEFAULT_TMP_PREFIX)

    exgroup.add_argument(
        "--cachedir", type=Text, default="",
        help="Directory to cache intermediate workflow outputs to avoid recomputing steps.")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument("--rm-tmpdir", action="store_true", default=True,
                         help="Delete intermediate temporary directories (default)",
                         dest="rm_tmpdir")

    exgroup.add_argument("--leave-tmpdir", action="store_false",
                         default=True, help="Do not delete intermediate temporary directories",
                         dest="rm_tmpdir")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument(
        "--move-outputs", action="store_const", const="move", default="move",
        help="Move output files to the workflow output directory and delete "
        "intermediate output directories (default).", dest="move_outputs")

    exgroup.add_argument("--leave-outputs", action="store_const", const="leave", default="move",
                         help="Leave output files in intermediate output directories.",
                         dest="move_outputs")

    exgroup.add_argument("--copy-outputs", action="store_const", const="copy", default="move",
                         help="Copy output files to the workflow output directory, don't delete intermediate output directories.",
                         dest="move_outputs")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument("--enable-pull", default=True, action="store_true",
                         help="Try to pull Docker images", dest="enable_pull")

    exgroup.add_argument("--disable-pull", default=True, action="store_false",
                         help="Do not try to pull Docker images", dest="enable_pull")

    parser.add_argument("--rdf-serializer",
                        help="Output RDF serialization format used by --print-rdf (one of turtle (default), n3, nt, xml)",
                        default="turtle")

    parser.add_argument("--eval-timeout",
                        help="Time to wait for a Javascript expression to evaluate before giving an error, default 20s.",
                        type=float,
                        default=20)

    provgroup = parser.add_argument_group("Options for recording provenance "
                                          "information of the execution")
    provgroup.add_argument("--provenance",
                           help="Save provenance to specified folder as a "
                           "Research Object that captures and aggregates "
                           "workflow execution and data products.",
                           type=Text)

    provgroup.add_argument("--enable-user-provenance", default=False,
                           action="store_true",
                           help="Record user account info as part of provenance.",
                           dest="user_provenance")
    provgroup.add_argument("--disable-user-provenance", default=False,
                           action="store_false",
                           help="Do not record user account info in provenance.",
                           dest="user_provenance")
    provgroup.add_argument("--enable-host-provenance", default=False,
                           action="store_true",
                           help="Record host info as part of provenance.",
                           dest="host_provenance")
    provgroup.add_argument("--disable-host-provenance", default=False,
                           action="store_false",
                           help="Do not record host info in provenance.",
                           dest="host_provenance")
    provgroup.add_argument(
        "--orcid", help="Record user ORCID identifier as part of "
        "provenance, e.g. https://orcid.org/0000-0002-1825-0097 "
        "or 0000-0002-1825-0097. Alternatively the environment variable "
        "ORCID may be set.", dest="orcid", default=os.environ.get("ORCID", ''),
        type=Text)
    provgroup.add_argument(
        "--full-name", help="Record full name of user as part of provenance, "
        "e.g. Josiah Carberry. You may need to use shell quotes to preserve "
        "spaces. Alternatively the environment variable CWL_FULL_NAME may "
        "be set.", dest="cwl_full_name", default=os.environ.get("CWL_FULL_NAME", ''),
        type=Text)

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument("--print-rdf", action="store_true",
                         help="Print corresponding RDF graph for workflow and exit")
    exgroup.add_argument("--print-dot", action="store_true",
                         help="Print workflow visualization in graphviz format and exit")
    exgroup.add_argument("--print-pre", action="store_true", help="Print CWL document after preprocessing.")
    exgroup.add_argument("--print-deps", action="store_true", help="Print CWL document dependencies.")
    exgroup.add_argument("--print-input-deps", action="store_true", help="Print input object document dependencies.")
    exgroup.add_argument("--pack", action="store_true", help="Combine components into single document and print.")
    exgroup.add_argument("--version", action="store_true", help="Print version and exit")
    exgroup.add_argument("--validate", action="store_true", help="Validate CWL document only.")
    exgroup.add_argument("--print-supported-versions", action="store_true", help="Print supported CWL specs.")
    exgroup.add_argument("--print-subgraph", action="store_true",
                         help="Print workflow subgraph that will execute "
                         "(can combine with --target)")
    exgroup.add_argument("--print-targets", action="store_true", help="Print targets (output parameters)")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument("--strict", action="store_true",
                         help="Strict validation (unrecognized or out of place fields are error)",
                         default=True, dest="strict")
    exgroup.add_argument("--non-strict", action="store_false", help="Lenient validation (ignore unrecognized fields)",
                         default=True, dest="strict")

    parser.add_argument("--skip-schemas", action="store_true",
            help="Skip loading of schemas", default=False, dest="skip_schemas")

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument("--verbose", action="store_true", help="Default logging")
    exgroup.add_argument("--quiet", action="store_true", help="Only print warnings and errors.")
    exgroup.add_argument("--debug", action="store_true", help="Print even more logging")

    parser.add_argument(
        "--strict-memory-limit", action="store_true", help="When running with "
        "software containers and the Docker engine, pass either the "
        "calculated memory allocation from ResourceRequirements or the "
        "default of 1 gigabyte to Docker's --memory option.")

    parser.add_argument("--timestamps", action="store_true", help="Add "
                        "timestamps to the errors, warnings, and "
                        "notifications.")
    parser.add_argument("--js-console", action="store_true", help="Enable javascript console output")
    parser.add_argument("--disable-js-validation", action="store_true", help="Disable javascript validation.")
    parser.add_argument("--js-hint-options-file",
                        type=Text,
                        help="File of options to pass to jshint."
                        "This includes the added option \"includewarnings\". ")
    dockergroup = parser.add_mutually_exclusive_group()
    dockergroup.add_argument("--user-space-docker-cmd", metavar="CMD",
                        help="(Linux/OS X only) Specify a user space docker "
                        "command (like udocker or dx-docker) that will be "
                        "used to call 'pull' and 'run'")
    dockergroup.add_argument("--singularity", action="store_true",
                             default=False, help="[experimental] Use "
                             "Singularity runtime for running containers. "
                             "Requires Singularity v2.3.2+ and Linux with kernel "
                             "version v3.18+ or with overlayfs support "
                             "backported.")
    dockergroup.add_argument("--shifter", action="store_true",
                             default=False, help="[experimental] Use "
                             "Shifter runtime for running containers. "
                             "Only tested on Cori at Nersc")
    dockergroup.add_argument("--no-container", action="store_false",
                             default=True, help="Do not execute jobs in a "
                             "Docker container, even when `DockerRequirement` "
                             "is specified under `hints`.",
                             dest="use_container")

    dependency_resolvers_configuration_help = argparse.SUPPRESS
    dependencies_directory_help = argparse.SUPPRESS
    use_biocontainers_help = argparse.SUPPRESS
    conda_dependencies = argparse.SUPPRESS

    if SOFTWARE_REQUIREMENTS_ENABLED:
        dependency_resolvers_configuration_help = "Dependency resolver configuration file describing how to adapt 'SoftwareRequirement' packages to current system."
        dependencies_directory_help = "Defaut root directory used by dependency resolvers configuration."
        use_biocontainers_help = "Use biocontainers for tools without an explicitly annotated Docker container."
        conda_dependencies = "Short cut to use Conda to resolve 'SoftwareRequirement' packages."

    parser.add_argument("--beta-dependency-resolvers-configuration", default=None, help=dependency_resolvers_configuration_help)
    parser.add_argument("--beta-dependencies-directory", default=None, help=dependencies_directory_help)
    parser.add_argument("--beta-use-biocontainers", default=None, help=use_biocontainers_help, action="store_true")
    parser.add_argument("--beta-conda-dependencies", default=None, help=conda_dependencies, action="store_true")

    parser.add_argument("--tool-help", action="store_true", help="Print command line help for tool")

    parser.add_argument("--relative-deps", choices=['primary', 'cwd'],
                        default="primary", help="When using --print-deps, print paths "
                                                "relative to primary file or current working directory.")

    parser.add_argument("--enable-dev", action="store_true",
                        help="Enable loading and running development versions "
                             "of CWL spec.", default=False)

    parser.add_argument("--enable-ext", action="store_true",
                        help="Enable loading and running cwltool extensions "
                             "to CWL spec.", default=False)

    parser.add_argument("--default-container",
                        help="Specify a default docker container that will be used if the workflow fails to specify one.")
    parser.add_argument("--no-match-user", action="store_true",
                        help="Disable passing the current uid to `docker run --user`")
    parser.add_argument("--custom-net", type=Text,
                        help="Passed to `docker run` as the '--net' "
                             "parameter when NetworkAccess is true.")
    parser.add_argument("--disable-validate", dest="do_validate",
                        action="store_false", default=True,
                        help=argparse.SUPPRESS)

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument("--enable-ga4gh-tool-registry", action="store_true", help="Enable resolution using GA4GH tool registry API",
                        dest="enable_ga4gh_tool_registry", default=True)
    exgroup.add_argument("--disable-ga4gh-tool-registry", action="store_false", help="Disable resolution using GA4GH tool registry API",
                        dest="enable_ga4gh_tool_registry", default=True)

    parser.add_argument("--add-ga4gh-tool-registry", action="append", help="Add a GA4GH tool registry endpoint to use for resolution, default %s" % ga4gh_tool_registries,
                        dest="ga4gh_tool_registries", default=[])

    parser.add_argument("--on-error",
                        help="Desired workflow behavior when a step fails.  One of 'stop' (do not submit any more steps) or "
                        "'continue' (may submit other steps that are not downstream from the error). Default is 'stop'.",
                        default="stop", choices=("stop", "continue"))

    exgroup = parser.add_mutually_exclusive_group()
    exgroup.add_argument("--compute-checksum", action="store_true", default=True,
                         help="Compute checksum of contents while collecting outputs",
                         dest="compute_checksum")
    exgroup.add_argument("--no-compute-checksum", action="store_false",
                         help="Do not compute checksum of contents while collecting outputs",
                         dest="compute_checksum")

    parser.add_argument("--relax-path-checks", action="store_true",
                        default=False, help="Relax requirements on path names to permit "
                        "spaces and hash characters.", dest="relax_path_checks")
    exgroup.add_argument("--make-template", action="store_true",
                         help="Generate a template input object")

    parser.add_argument("--force-docker-pull", action="store_true",
                        default=False, help="Pull latest docker image even if"
                                            " it is locally present", dest="force_docker_pull")
    parser.add_argument("--no-read-only", action="store_true",
                        default=False, help="Do not set root directory in the"
                                            " container as read-only", dest="no_read_only")

    parser.add_argument("--overrides", type=str,
                        default=None, help="Read process requirement overrides from file.")

    parser.add_argument("--target", "-t", action="append",
                        help="Only execute steps that contribute to "
                        "listed targets (can provide more than once).")

    parser.add_argument("ceci_configuration", type=Text, nargs="?", default=None,
            metavar='ceci_configuration', help="path or URL to a ceci Workflow")

    return parser
