FROM golang:1.7-alpine3.5

RUN mkdir -p "$GOPATH/src"

ADD . "$GOPATH/src/app"

WORKDIR "$GOPATH/src/app"

RUN apk --no-cache --virtual .build-dependencies add git \
  && cd $GOPATH/src/app \
  && BUILDINFO_PACKAGE="github.com/Financial-Times/service-status-go/buildinfo." \
  && VERSION="version=$(git describe --tag --always 2> /dev/null)" \
  && DATETIME="dateTime=$(date -u +%Y%m%d%H%M%S)" \
  && REPOSITORY="repository=$(git config --get remote.origin.url)" \
  && REVISION="revision=$(git rev-parse HEAD)" \
  && BUILDER="builder=$(go version)" \
  && LDFLAGS="-X '"${BUILDINFO_PACKAGE}$VERSION"' -X '"${BUILDINFO_PACKAGE}$DATETIME"' -X '"${BUILDINFO_PACKAGE}$REPOSITORY"' -X '"${BUILDINFO_PACKAGE}$REVISION"' -X '"${BUILDINFO_PACKAGE}$BUILDER"'" \
  && go-wrapper download \
  && go-wrapper install -ldflags="${LDFLAGS}" \
  && apk del .build-dependencies \
  && rm -rf $GOPATH/src $GOPATH/pkg /usr/local/go

CMD ["go-wrapper", "run"]
