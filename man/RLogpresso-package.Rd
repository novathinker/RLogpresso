\name{RLogpresso-package}
\alias{RLogpresso-package}
\alias{RLogpresso}
\docType{package}
\title{
RLogpresso
}
\description{
Communicate with Araqne LogDB Library
}
\details{
\tabular{ll}{
Package: \tab RLogpresso\cr
Type: \tab Package\cr
Version: \tab 1.0.0\cr
Date: \tab 2014-12-08\cr
License: \tab Apache License, Version 2.0\cr
}
~~ An overview of how to use the package, including the most important ~~
~~ functions ~~
}
\author{
Maintainer: delmitz <kurosmitz@gmail.com>
}
\references{
~~ Literature or other references for background information ~~
}
\keyword{ package }
\seealso{
http://eediom.com/logpresso/
}
\examples{
client <- RLogpresso.create()
client$connect("localhost", 80, "araqne", "password")

client$tables()
client$query("YOUR_QUERY_STRING_HERE")

client$disconnect()
}
