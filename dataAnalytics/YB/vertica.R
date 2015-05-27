parse_JSON <- function(x,k) {

    library(RJSON)

    row <- ls(x)

}

parse_JSON_factory <- function() {
    list(name=parse_JSON,
        udxtype=c("transform")
        intype=c("character")
        outtype=c("character"))
}