package eaglemq

#define MESSAGE_BUFFER_SIZE 256

// #define EG_NOTUSED(X) ((void)X)
// #define BIT_SET(a, b) ((a) |= (1UL<<(b)))
// #define BIT_CHECK(a, b) ((a) & (1UL<<(b)))


#define TO_LOWER(c) (unsigned char)(c | 0x20)
#define IS_ALPHA(c) (TO_LOWER(c) >= 'a' && TO_LOWER(c) <= 'z')
#define IS_NUM(c) ((c) >= '0' && (c) <= '9')
#define IS_ALPHANUM(c) (IS_ALPHA(c) || IS_NUM(c))
#define IS_EXTRA1(c) ((c) == '_' || (c) == '-' || (c) == '.')
#define IS_EXTRA2(c) ((c) == '*' || (c) == '?' || (c) == '[' || (c) == ']' || (c) == '\\')
#define IS_EXTRA3(c) (IS_EXTRA1(c) || IS_EXTRA2(c))

#define strlenz(s) (strlen(s) + 1)

typedef enum { INFO_LEVEL, WARNING_LEVEL, FATAL_LEVEL, LOG_ONLY_LEVEL } message_level;

void enable_log(const char *logfile);
void disable_log();
void warning(const char *fmt,...);
void info(const char *fmt,...);
void fatal(const char *fmt,...);
void wlog(const char *fmt,...);

int pattern_match_length(const char *string, int slength, const char *pattern, int plength, int nocase);
int pattern_match(const char *string, const char *pattern, int nocase);

uint64_t make_message_tag(uint32_t msg_counter, uint32_t time);

long long memtoll(const char *value, int *err);

int check_input_buffer1(char *buffer, size_t size);
int check_input_buffer2(char *buffer, size_t size);
int check_input_buffer3(char *buffer, size_t size);

int log_init = 0;
FILE *fd;


// mstime returns the current time as a Unix timestamp containing the number
// of milliseconds elapsed since January 1, 1970 UTC.
func mstime() int64 {
	return time.Now().UnixNano()/1000
}

void enable_log(const char *logfile)
{
	fd = fopen(logfile, "a");

	if (fd == NULL) {
		warning("Error create log file");
		return;
	}

	log_init = 1;
}

void disable_log()
{
	if (log_init) {
		log_init = 0;
		fclose(fd);
	}
}

static void output_message(int level, const char *fmt, va_list args)
{
	time_t now_time;
	char msg[MESSAGE_BUFFER_SIZE];
	char log[MESSAGE_BUFFER_SIZE];

	vsnprintf(msg, sizeof(msg), fmt, args);

	if (level != LOG_ONLY_LEVEL)
	{
		if (level == FATAL_LEVEL || level == WARNING_LEVEL) {
			fprintf(stderr, "%s\n", msg);
		} else {
			fprintf(stdout, "%s\n", msg);
		}
	}

	if (log_init) {
		time(&now_time);
		sprintf(log, "%s%s\n", ctime(&now_time), msg);
		fputs(log, fd);
		fflush(fd);
	}
}

void warning(const char *fmt,...)
{
	va_list args;

	va_start(args, fmt);
	output_message(WARNING_LEVEL, fmt, args);
	va_end(args);
}

void info(const char *fmt,...)
{
	va_list args;

	va_start(args, fmt);
	output_message(INFO_LEVEL, fmt, args);
	va_end(args);
}

void fatal(const char *fmt,...)
{
	va_list args;

	va_start(args, fmt);
	output_message(FATAL_LEVEL, fmt, args);
	va_end(args);

	exit(EG_STATUS_ERR);
}

void wlog(const char *fmt,...)
{
	va_list args;

	va_start(args, fmt);
	output_message(LOG_ONLY_LEVEL, fmt, args);
	va_end(args);
}

/* Taken from Redis */
int pattern_match_length(const char *string, int slength, const char *pattern, int plength, int nocase)
{
	while(plength)
	{
		switch(pattern[0])
		{
			case '*':
				while (pattern[1] == '*') {
					pattern++;
					plength--;
				}

				if (plength == 1)
					return 1;

				while(slength)
				{
					if (pattern_match_length(pattern + 1, plength - 1, string, slength, nocase))
						return 1;

					string++;
					slength--;
				}

				return 0;
				break;

			case '?':
				if (slength == 0)
					return 0;

				string++;
				slength--;

				break;

			case '[':
			{
				int not, match;

				pattern++;
				plength--;
				not = pattern[0] == '^';

				if (not) {
					pattern++;
					plength--;
				}

				match = 0;

				while(1) {
					if (pattern[0] == '\\')
					{
						pattern++;
						plength--;

						if (pattern[0] == string[0])
							match = 1;
					} else if (pattern[0] == ']') {
						break;
					} else if (plength == 0) {
						pattern--;
						plength++;
						break;
					} else if (pattern[1] == '-' && plength >= 3) {
						int start = pattern[0];
						int end = pattern[2];
						int c = string[0];

						if (start > end)
						{
							int t = start;
							start = end;
							end = t;
						}

						if (nocase) {
							start = tolower(start);
							end = tolower(end);
							c = tolower(c);
						}

						pattern += 2;
						plength -= 2;

						if (c >= start && c <= end)
							match = 1;
					}
					else
					{
						if (!nocase) {
							if (pattern[0] == string[0])
								match = 1;
						} else {
							if (tolower((int)pattern[0]) == tolower((int)string[0]))
								match = 1;
						}
				}

				pattern++;
				plength--;
			}

			if (not)
				match = !match;

			if (!match)
				return 0;

			string++;
			slength--;

			break;
			}

		case '\\':
			if (plength >= 2) {
				pattern++;
				plength--;
			}

		default:
			if (!nocase) {
				if (pattern[0] != string[0])
					return 0;
			} else {
				if (tolower((int)pattern[0]) != tolower((int)string[0]))
					return 0;
			}

			string++;
			slength--;

			break;
		}

		pattern++;
		plength--;

		if (slength == 0)
		{
			while(*pattern == '*') {
				pattern++;
				plength--;
			}
			break;
		}
	}

	if (plength == 0 && slength == 0)
		return 1;

	return 0;
}

int pattern_match(const char *string, const char *pattern, int nocase)
{
	return pattern_match_length(string, strlen(string), pattern, strlen(pattern), nocase);
}

uint64_t make_message_tag(uint32_t msg_counter, uint32_t time)
{
	uint64_t id = 0;

	id |= time;
	id = id << 32;
	id |= msg_counter;

	return id;
}

long long memtoll(const char *value, int *err)
{
	char buffer[128];
	const char *ptr;
	unsigned int digits;
	long mul;

	*err = 0;

	ptr = value;
	while (*ptr && isdigit(*ptr)) {
		ptr++;
	}

	if (*ptr == '\0' || !strcasecmp(ptr, "b")) {
		mul = 1;
	} else if (!strcasecmp(ptr, "k")) {
		mul = 1000;
	} else if (!strcasecmp(ptr, "m")) {
		mul = 1000 * 1000;
	} else if (!strcasecmp(ptr, "g")) {
		mul = 1000L * 1000 * 1000;
	} else {
		*err = 1;
		mul = 1;
	}

	digits = ptr - value;
	if (digits >= sizeof(buffer)) {
		*err = 1;
		return LLONG_MAX;
	}

	memcpy(buffer, value, digits);
	buffer[digits] = '\0';

	return strtoll(buffer, NULL, 10) * mul;
}

int check_input_buffer1(char *buffer, size_t size)
{
	char *ptr = buffer, *end_ptr = buffer + size;
	int length = 0;

	while (ptr < end_ptr) {
		if (*++ptr == '\0') {
			length = ptr - buffer;
			break;
		}
	}

	if (!length) {
		return 0;
	}

	for (ptr = buffer; ptr < (buffer + length); ptr++)
	{
		if (!IS_ALPHANUM(*ptr) && !IS_EXTRA1(*ptr)) {
			return 0;
		}
	}

	return length;
}

int check_input_buffer2(char *buffer, size_t size)
{
	char *ptr = buffer, *end_ptr = buffer + size;
	int length = 0;
	int extra = 0;

	while (ptr < end_ptr) {
		if (*++ptr == '\0') {
			length = ptr - buffer;
			break;
		}
	}

	if (!length) {
		return 0;
	}

	if (!IS_ALPHA(*buffer) && *buffer != '_' && *buffer != '.') {
		return 0;
	}

	if (*buffer == '_' || *buffer == '.') {
		extra = 1;
	}

	for (ptr = buffer + 1; ptr < (buffer + length - 1); ptr++)
	{
		if (IS_EXTRA1(*ptr) && extra) {
			return 0;
		}

		if (!IS_ALPHANUM(*ptr) && !IS_EXTRA1(*ptr)) {
			return 0;
		}

		extra = 0;
		if (IS_EXTRA1(*ptr)) {
			extra = 1;
		}
	}

	return length;
}

int check_input_buffer3(char *buffer, size_t size)
{
	char *ptr = buffer, *end_ptr = buffer + size;
	int length = 0;

	while (ptr < end_ptr) {
		if (*++ptr == '\0') {
			length = ptr - buffer;
			break;
		}
	}

	if (!length) {
		return 0;
	}

	for (ptr = buffer; ptr < (buffer + length); ptr++)
	{
		if (!IS_ALPHANUM(*ptr) && !IS_EXTRA3(*ptr)) {
			return 0;
		}
	}

	return length;
}
