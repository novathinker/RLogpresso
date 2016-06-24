\name{RLogpresso.create}
\alias{RLogpresso.create}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Create Araqne Log Databse Client
}
\description{
Create Communicator with Araqne LogDB
}
\usage{
RLogpresso.create()
}
%- maybe also 'usage' for other objects documented here.
\details{
Returning Object has following functions.

 * connect(host, port=8888, loginName, password="", connectTimeout=0, readTimeout=10000)\cr
 * disconnect()\cr
 * query(query, interval=1)\cr
 * createQuery(query)\cr
 * startQuery(id)\cr
 * stopQuery(id)\cr
 * removeQuery(id)\cr
 * fetch(id, offset, limit)\cr
 * queries()\cr
 * queryStatus(id)\cr
 * logFileTypes()\cr
 * createTable(name, logFileType)\cr
 * dropTable(name)\cr
 * tables()\cr
 * tableInfo(name)\cr
}
\value{
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
delmitz
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
client <- RLogpresso.create()
client$connect("localhost", 80, "araqne", "password")

client$tables()
client$query("YOUR_QUERY_STRING_HERE")

client$disconnect()
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ ~kwd1 }
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
