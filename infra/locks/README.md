# Runtime dependency locks (task-index P8-W02).
#
# Regenerate with:
#   uv pip compile requirements-<runtime>.txt \
#     -o infra/locks/requirements-<runtime>.lock \
#     --generate-hashes --no-annotate
#
# Install inside images with:
#   python -m pip install --require-hashes -r infra/locks/requirements-<runtime>.lock
#
# Dockerfile.api installs requirements-api.lock. Dockerfile.training.cpu
# installs both requirements-api.lock and requirements-training-cpu.lock:
# training imports shared API modules, including Kubernetes and SQLGlot.
# Shared pins must agree. The CPU lock contains the hashed torch==2.9.1+cpu
# wheel served by https://download.pytorch.org/whl/cpu.
#
# The images declare SCEPTRE_RUNTIME_PROFILE=api or cpu. Catalog publication
# and consumption verify every installed distribution/version against that
# image's complete reviewed inventory, including the pinned Python and pip.
# Catalog identity binds both inventories, both complete lock hashes and the
# complete application Python source fingerprint, so
# the paired images agree without ignoring additional or changed packages.
# Production/staging rejects missing or unknown profiles. An unconfigured
# local interpreter retains its full development inventory fingerprint and
# does not qualify a production image.
#
# Before release tags are published, scripts/validate_runtime_pair.py checks
# actual API/CPU image catalogs, source/lock hashes, imports and a small model
# fit. This check does not qualify GPU runtimes, signatures or load capacity.
