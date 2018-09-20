VERSION ?= 0.3
DISTRO ?= trusty
BUILD_PREFIX ?= /

DOCKERFILE := Dockerfile.${DISTRO}
DOCKER_TAG := govuk/datascrubber:${VERSION}

INSTALLPATH := ${BUILD_PREFIX}/opt/datascrubber
CMDPATH := ${BUILD_PREFIX}/usr/bin
PKGVER := ${VERSION}~${DISTRO}
PACKAGE := govuk-datascrubber_${PKGVER}_amd64.deb
DISTDIR := ${HOME}/dist

.PHONY: container
container:
	docker build \
		--tag $(DOCKER_TAG) \
		--file $(DOCKERFILE) .

.PHONY: deb
deb: container
	docker run \
		--mount type=bind,source=$(PWD)/dist,target=/host \
		${DOCKER_TAG} \
		rsync -vr /home/build/dist/ /host/

.PHONY: venv
venv: ${INSTALLPATH}/bin/activate

${INSTALLPATH}/bin/activate:
	test -d ${INSTALLPATH}/bin/activate || virtualenv --python=python3 ${INSTALLPATH}
	. ${INSTALLPATH}/bin/activate \
		&& pip install --upgrade pip \
		&& pip install -r requirements.txt \
		&& pip install .

.PHONY: command
command: ${CMDPATH}/datascrubber

${CMDPATH}/datascrubber:
	mkdir -p ${CMDPATH}
	install -m 0755 bin/datascrubber ${CMDPATH}/datascrubber
	sed --in-place \
		--expression '1d' \
		--expression '2i #!/opt/datascrubber/bin/python3' ${CMDPATH}/datascrubber

${DISTDIR}:
	mkdir -p ${DISTDIR}

${DISTDIR}/${PACKAGE}: ${DISTDIR}
	fpm --name govuk-datascrubber \
		-s dir -t deb \
		-C ${BUILD_PREFIX} \
		--version ${PKGVER} \
		--license MIT \
		--vendor 'GOV.UK Reliability Engineering' \
		--maintainer 'GOV.UK Reliability Engineering <reliability-engineering@digital.cabinet-office.gov.uk>' \
		--url 'https://github.com/alphagov/govuk-datascrubber' \
		--description 'Removes sensitive data from GOV.UK databases' \
		--force \
		--package ${DISTDIR}/${PACKAGE}

.PHONY: install
install: venv command

.PHONY: package
package: install ${DISTDIR}/${PACKAGE}
