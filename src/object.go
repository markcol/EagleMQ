package eaglemq

#define EG_OBJECT_SIZE(x) ((x)->size)
#define EG_OBJECT_DATA(x) ((x)->data)
#define EG_OBJECT_RESET_REFCOUNT(x) ((x)->refcount = 0)

type Object struct {
	data interface{}
	size size_t
	refcount uint
}

// ALL FUNCTIONS PUBLIC

Object *create_object(void *ptr, size_t size)
{
	Object *object = (Object*)xmalloc(sizeof(*object));

	object->data = ptr;
	object->size = size;
	object->refcount = 1;

	return object;
}

Object *create_dup_object(void *ptr, size_t size)
{
	Object *object = (Object*)xmalloc(sizeof(*object));

	object->data = xmalloc(size);
	object->size = size;
	object->refcount = 1;

	memcpy(object->data, ptr, size);

	return object;
}

void release_object(Object *object)
{
	xfree(object->data);
	xfree(object);
}

void increment_references_count(Object *object)
{
	object->refcount++;
}

void decrement_references_count(Object *object)
{
	if (object->refcount <= 1) {
		release_object(object);
	} else {
		object->refcount--;
	}
}

void free_object_list_handler(void *ptr)
{
	decrement_references_count(ptr);
}
