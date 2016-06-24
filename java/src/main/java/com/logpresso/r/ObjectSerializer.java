package com.logpresso.r;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class ObjectSerializer {
	private static ThreadLocal<SimpleDateFormat> df = new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
	};

	public static String[] map(Map<String, Object> m) {
		String[] ret = new String[m.size() * 2];
		int idx = 0;
		for (String key : m.keySet()) {
			Object val = m.get(key);
			if (val != null) {
				if (val instanceof Date)
					val = df.get().format(val);
				else
					val = val.toString();
			}

			ret[idx++] = key;
			ret[idx++] = (String) val;
		}
		return ret;
	}
}
