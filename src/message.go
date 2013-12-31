/*
   Copyright (c) 2012, Stanislav Yakush(st.yakush@yandex.ru)
   All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the EagleMQ nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
   ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY
   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
   (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
   LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#ifndef __MESSAGE_H__
#define __MESSAGE_H__

#include <stdint.h>

#include "eagle.h"
#include "object.h"

#define EG_MESSAGE_OBJECT(m) ((m)->value)
#define EG_MESSAGE_VALUE(m) ((m)->value->data)
#define EG_MESSAGE_SIZE(m) ((m)->value->size)

#define EG_MESSAGE_GET_DATA(m, i) ((m)->data[i])
#define EG_MESSAGE_SET_DATA(m, i, v) ((m)->data[i] = (v))

#define EG_MESSAGE_GET_TAG(m) ((m)->tag)
#define EG_MESSAGE_SET_TAG(m, v) ((m)->tag = (v))

#define EG_MESSAGE_GET_CONFIRM_TIME(m) ((m)->confirm)
#define EG_MESSAGE_SET_CONFIRM_TIME(m, v) ((m)->confirm = (v))

#define EG_MESSAGE_GET_EXPIRATION_TIME(m) ((m)->expiration)
#define EG_MESSAGE_SET_EXPIRATION_TIME(m, v) ((m)->expiration = (v))

typedef struct Message {
	Object *value;
	uint64_t tag;
	uint32_t confirm;
	uint32_t expiration;
	void *data[2];
} Message;

Message *create_message(Object *data, uint64_t tag, uint32_t expiration);
void release_message(Message *msg);
void free_message_list_handler(void *ptr);

#endif
/*
   Copyright (c) 2012, Stanislav Yakush(st.yakush@yandex.ru)
   All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the EagleMQ nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
   ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY
   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
   (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
   LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include <string.h>

#include "eagle.h"
#include "message.h"
#include "list.h"
#include "xmalloc.h"

Message *create_message(Object *data, uint64_t tag, uint32_t expiration)
{
	Message *msg = (Message*)xcalloc(sizeof(*msg));

	msg->value = data;
	msg->tag = tag;
	msg->confirm = 0;
	msg->expiration = expiration;

	return msg;
}

void release_message(Message *msg)
{
	decrement_references_count(msg->value);
	xfree(msg);
}

void free_message_list_handler(void *ptr)
{
	release_message(ptr);
}
