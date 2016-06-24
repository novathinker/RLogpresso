package com.logpresso.r;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.logpresso.client.Logpresso;
import com.logpresso.client.Query;
import com.logpresso.client.Tuple;
import com.logpresso.client.StreamingResultSet;

import au.com.bytecode.opencsv.CSVWriter;

public class QueryManager2 implements StreamingResultSet {
	private Logpresso client;
	private int id;
	private boolean isEnd = false;
	private File file;
	private CSVWriter writer;
	private int count = 0;
	private List<String> headers = new ArrayList<String>();
	private String[] newline = new String[0];

	public QueryManager2(Logpresso client, String query) throws IOException {
		this.client = client;
		this.id = client.createQuery(query, this);
		this.file = File.createTempFile("rlogpresso" + id + "_", ".tmp");
		this.writer = new CSVWriter(new PrintWriter(file));
		client.startQuery(id);
	}

	public boolean isEnd() {
		if (isEnd) {
			try {
				client.removeQuery(id);
			} catch (IOException e) {
			}
		}
		return isEnd;
	}

	public String filename() {
		return file.getAbsolutePath();
	}

	public int rowCount() {
		return count;
	}

	public String[] headers() {
		return headers.toArray(new String[0]);
	}

	public void stop() {
		try {
			client.stopQuery(id);
		} catch (IOException e) {
		}
		try {
			client.removeQuery(id);
		} catch (IOException e1) {
		}
		try {
			writer.close();
		} catch (IOException e) {
		}
		file.delete();
	}

	@Override
	public void onRows(Query query, List<Tuple> rows, boolean last) {
		for (Tuple row : rows) {
			Map<String, Object> m = row.toMap();
			if (!headers.containsAll(m.keySet())) {
				Set<String> newKeys = new HashSet<String>(m.keySet());
				newKeys.removeAll(headers);
				headers.addAll(newKeys);
				newline = new String[headers.size()];
			}

			int idx = 0;
			for (String key : headers) {
				Object value = m.get(key);
				newline[idx++] = (value != null) ? value.toString() : null;
			}
			writer.writeNext(newline);
			count++;
		}

		if (last) {
			try {
				writer.close();
			} catch (IOException e) {
			}
			isEnd = true;
		}
	}
}
