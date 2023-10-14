FROM gradle:focal as build

WORKDIR 	/app
COPY		. .
RUN		gradle clean
RUN		gradle build


FROM scratch
COPY --from=build /app/lib/build/libs/* .
COPY --from=build /app/lib/build/deps/* .