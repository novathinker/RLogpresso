logpresso <- new.env()
logpresso$initialized <- FALSE
logpresso$client <- "1.0.0-2"
logpresso$library <- "1.0.0"

.init <- function() {
	library(rJava)
	.jinit()
	.jaddClassPath(paste0(system.file(package="RLogpresso"), "/exec/logpresso-sdk-java-", logpresso$client , "-package.jar"))
	.jaddClassPath(paste0(system.file(package="RLogpresso"), "/exec/RLogpresso-1.0.0.jar"))
	
	ca <- .jnew("org/apache/log4j/ConsoleAppender", .jcast(.jnew("org/apache/log4j/PatternLayout"), "org/apache/log4j/Layout"))
	.jcall(ca, "V", "setThreshold", .jcast(.jfield("org/apache/log4j/Level", , "INFO"), "org/apache/log4j/Priority"))
	.jcall(J("org.apache.log4j.BasicConfigurator"), "V", "configure", .jcast(ca, "org/apache/log4j/Appender"))

	logpresso$initialized <- TRUE
}

RLogpresso.version <- function() {
	if (logpresso$initialized == FALSE)
		.init()
	jvm <- J("java.lang.System")$getProperty("java.version")
	client <- logpresso$client
	rlogpresso <- logpresso$library
	return(c(JVM=jvm, java_client=client, RLogpresso=rlogpresso))
}

RLogpresso.create <- function(output=TRUE) {
	if (logpresso$initialized == FALSE)
		.init()
	client <- new.env()
	client$print <- output
	client$jobj <- NULL

	connect_check <- function(client) {
		if (is.null(client$jobj))
			stop("connect first")
	}
	
	client$connect <- function(host, port=8888, loginName, password="", connectTimeout=0, readTimeout=10000) {
		if (!is.null(client$jobj))
			client$disconnect()
		jobj <- .jnew("com/logpresso/client/Logpresso")
		.call(jobj, "V", "connect", host, as.integer(port), loginName, password, as.integer(connectTimeout), as.integer(readTimeout))
		client$jobj <- jobj
		if (client$print) message("connected to ", host, ":", port, " as ", loginName)
	}
	client$disconnect <- function() {
		connect_check(client)
		.call(client$jobj, "V", "close")
		client$jobj <- NULL
		if (client$print) message("disconnected")
	}
	client$query <- function(query, interval=1) {
		connect_check(client)
		qm <- .jnew("com/logpresso/r/QueryManager", client$jobj, query)
		stop <- function(ex) {
			.call(qm, "V", "stop")
			print(ex)
		}
		readcsv <- function(qm, data) {
			filename <- .call(qm, "S", "filename")
			if (file.info(filename)$size > 0) {
				nrows <- .call(qm, "I", "rowCount")
				headers <- .call(qm, "[S", "headers")
				newdata <- read.csv(filename, header=FALSE, col.names=headers, nrows=nrows)
				if (length(data) > 0) {
					for ( name in colnames(newdata) ) {
						if (!(name %in% colnames(data))) {
							data[name] <- NA
						}
					}
				}
				data <- rbind(data, newdata)
			}
			file.remove(filename)
			return(data)
		}

		tryCatch({
			data <- data.frame()
			while (!.call(qm, "Z", "isEnd")) {
				Sys.sleep(interval);
				data <- readcsv(qm, data)
			}
			data <- readcsv(qm, data)
			
			headers <- .call(qm, "[S", "headers")
			colnames(data) <- headers
			
			order <- .call(qm, "[S", "fieldOrder")
			if (!is.null(order)) {
				for ( name in order ) {
					if (!(name %in% colnames(data))) {
						# create undefined column
						data[name] <- NA
					}
				}
				data <- data[order] # column reordering
			}
			
			return(data)
		}, interrupt=stop, error=stop)
	}
	client$createQuery <- function(query) {
		connect_check(client)
		id <- .call(client$jobj, "I", "createQuery", query)
		if (client$print) message("created query ", id)
		return(id)
	}
	client$startQuery <- function(id) {
		connect_check(client)
		.call(client$jobj, "V", "startQuery", as.integer(id))
		if (client$print) message("started query ", id)
	}
	client$stopQuery <- function(id) {
		connect_check(client)
		.call(client$jobj, "V", "stopQuery", as.integer(id))
		if (client$print) message("stopped query ", id)
	}
	client$removeQuery <- function(id) {
		connect_check(client)
		.call(client$jobj, "V", "removeQuery", as.integer(id))
		if (client$print) message("removed query ", id)
	}
	client$fetch <- function(id, offset, limit) {
		connect_check(client)
		qm <- .jnew("com/logpresso/r/QueryManager", client$jobj, as.integer(id), .jlong(offset), as.integer(limit))
		filename <- .call(qm, "S", "filename")
		nrows <- .call(qm, "I", "rowCount")
		headers <- .call(qm, "[S", "headers")
		if (file.info(filename)$size == 0)
			return(NULL)
		data <- read.csv(filename, header=FALSE, col.names=headers, nrows=nrows)
		file.remove(filename)
		return(data)
	}
	client$queries <- function() {
		connect_check(client)
		queries <- .call(client$jobj, "Ljava/util/List;", "getQueries", parse="[Query")
		return(queries)
	}
	client$getQueryStatus <- function(id) {
		connect_check(client)
		query <- .call(client$jobj, "Lcom/logpresso/client/Query;", "getQuery", as.integer(id), parse="Query")
		return(query)
	}
	client$createTable <- function(name, engine) {
		connect_check(client)
		.call(client$jobj, "V", "createTable", name, engine)
		if (client$print) message("created")
	}
	client$dropTable <- function(name) {
		connect_check(client)
		.call(client$jobj, "V", "dropTable", name)
		if (client$print) message("dropped")
	}
	client$getTables <- function() {
		connect_check(client)
		return(.call(client$jobj, "Ljava/util/List;", "listTables", parse="[TableSchema"))
	}
	client$getTableSchema <- function(name) {
		connect_check(client)
		return(.call(client$jobj, "Lcom/logpresso/client/TableSchema;", "getTableSchema", name, parse="TableSchema"))
	}
	
	return(client)
}

.call <- function(jobj, ret_type, name, ..., parse=NULL) {
	ret <- .jcall(jobj, ret_type, name, ..., check=FALSE)
	if (!is.null(e <- .jgetEx())) {
		.jcheck(silent=TRUE)
		stop(.jcall(e, "S", "getMessage"))
	}

	if (is.null(ret))
		return(NULL)
	if (nchar(ret_type) == 1)
		return(ret)
	if (substr(ret_type, 1, 1) != "[" && .jinstanceof(ret, J("java.util.List")))
		ret <- as.list(ret)
	if (!is.null(parse))
		return(.parse(ret, parse))
	return(ret)
}

.parse <- function(obj, type) {
	if (is.null(obj))
		return(NULL)

	if (is.list(obj)) {
		if (length(obj) == 0)
			return("")

		ret <- NULL
		for ( i in 1 : length(obj) ) {
			tmp <- .parse(obj[[i]], type)
			if (is.null(ret)) {
				ret <- as.data.frame(matrix(nrow=length(obj), ncol=length(tmp)))
				colnames(ret) <- names(tmp)
			}
			ret[i,] <- tmp
		}
		return(ret)
	}

	if (type == "Long") {
		return(.call(obj, "J", "longValue"))
	} else if (type == "String") {
		return(.call(obj, "S", "toString"))
	} else if (type == "Date") {
		time <- floor(.call(obj, "J", "getTime") / 1000)
		if (time == 0)
			return("")
		return(toString(as.POSIXct(time, origin="1970-01-01")))
	} else if (type == "Map") {
		data <- .call(J("com.logpresso.r.ObjectSerializer"), "[S", "map", obj)
		ret <- as.data.frame(matrix(nrow=length(data) / 2, ncol=2))
		colnames(ret) <- c("name", "value")
		if (length(data) == 0)
			return(ret)
		for ( i in 1 : (length(data) / 2) )
			ret[i,] <- c(name=data[i * 2 - 1], value=data[i * 2])
		return(ret)
	}

	if (type == "[Query") {
		id <- .call(obj, "I", "getId")
		qstr <- .call(obj, "S", "getQueryString")
		status <- .call(obj, "S", "getStatus")
		loaded <- .call(obj, "J", "getLoadedCount")
		bg <- .call(obj, "Z", "isBackground")
		start <- .call(obj, "Ljava/util/Date;", "getStartTime", parse="Date")
		if (is.null(start))
			start <- ""
		finish <- .call(obj, "Ljava/util/Date;", "getFinishTime", parse="Date")
		if (is.null(finish))
			finish <- ""
		return(c(id=id, query_string=qstr, status=status, loaded_count=loaded, is_background=bg, start_time=start, finish_time=finish))
	} else if (type == "Query") {
		id <- .call(obj, "I", "getId")
		qstr <- .call(obj, "S", "getQueryString")
		status <- .call(obj, "S", "getStatus")
		loaded <- .call(obj, "J", "getLoadedCount")
		bg <- .call(obj, "Z", "isBackground")
		start <- .call(obj, "Ljava/util/Date;", "getStartTime", parse="Date")
		finish <- .call(obj, "Ljava/util/Date;", "getFinishTime", parse="Date")
		elapsed <- .call(obj, "Ljava/lang/Long;", "getElapsed", parse="Long")
		cmds <- .call(obj, "Ljava/util/List;", "getCommands", parse="QueryCommand")
		errcode <- .call(obj, "Ljava/lang/Integer;", "getErrorCode")
		errdetail <- .call(obj, "S", "getErrorDetail")
		return(list(id=id, query_string=qstr, status=status, loaded_count=loaded, is_background=bg, start_time=start, finish_time=finish,
			elapsed=elapsed, commands=cmds, error_code=errcode, error_detail=errdetail))
	} else if (type == "Fetch") {
		data <- .jevalArray(obj)
		ret <- list()
		for ( i in 1 : (length(data) / 2) )
			ret[[data[i * 2 - 1]]] <- data[i * 2]
		return(ret)
	} else if (type == "[TableSchema") {
		return(c(table_name=.call(obj, "S", "getName")))
	} else if (type == "TableSchema") {
		name <- .call(obj, "S", "getName")
		metadata <- .call(obj, "Ljava/util/Map;", "getMetadata", parse="Map")
		return(list(table_name=name, metadata=metadata))
	} else if (type == "TableSchemaInfo") {
		fields <- .call(obj, "Ljava/util/List;", "getFieldDefinitions")
		ret <- as.data.frame(matrix(ncol=3, nrow=length(fields)))
		colnames(ret) <- c("name", "type", "length")
		if (!is.list(fields) || length(fields) == 0)
			return(ret)
		for ( i in 1 : length(fields) ) {
			name <- .call(fields[[i]], "S", "getName")
			type <- .call(fields[[i]], "S", "getType")
			len <- .call(fields[[i]], "I", "getLength")
			ret[i,] <- c(name=name, type=type, length=len)
		}
		return(ret)
	} else if (type == "QueryCommand") {
		name <- .call(obj, "S", "getName")
		cmd <- .call(obj, "S", "getCommand")
		status <- .call(obj, "S", "getStatus")
		cnt <- .call(obj, "J", "getPushCount")
		return(c(name=name, command=cmd, status=status, push_count=cnt))
	}
}
